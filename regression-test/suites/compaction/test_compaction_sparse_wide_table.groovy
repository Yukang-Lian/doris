// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Random
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_compaction_sparse_wide_table") {
    // ========== TEST MODE ==========
    def testFlameGraphOnly = false  // Set to true to only test flame graph
    def testPid = null              // Set to specific pid, or null to read from bePidFile
    // ===============================

    def totalColumns = 5000
    def rowsPerLoad = 20000
    def loadTimes = 50
    def parallelism = 20
    def genParallelism = 50
    //def sparsityLevels = [1, 5, 10, 20, 50, 100]
    def sparsityLevels = [1]
    def enableCache = true
    def cleanIncompleteCache = true
    def dropTablesAfterLoad = false
    def triggerCompaction = true  // whether to trigger compaction test
    def compactionTimeoutSeconds = 3600  // compaction timeout in seconds (1 hour)
    // BE log file path for parsing precise compaction cost time
    // Set to null to skip log parsing, only use client-side timing
    def beLogFilePath = "/mnt/disk2/lianyukang/output/be/log/be.INFO"

    // Flame graph settings
    def enableFlameGraph = true
    def bePidFile = "/mnt/disk2/lianyukang/doris/output/be/bin/be.pid"
    def flameGraphDir = "/mnt/disk2/lianyukang/perf/flame-graph"

    def columnSeparator = ","
    def nullToken = "\\N"
    def baseTypes = [
        [type: "TINYINT", value: "1"],
        [type: "SMALLINT", value: "2"],
        [type: "INT", value: "3"],
        [type: "BIGINT", value: "4"],
        [type: "LARGEINT", value: "123456789012345678"],
        [type: "FLOAT", value: "1.25"],
        [type: "DOUBLE", value: "3.14159"],
        [type: "DECIMALV3(18,6)", value: "12345.678901"],
        [type: "CHAR(10)", value: "charval"],
        [type: "VARCHAR(20)", value: "varcharval"],
        [type: "STRING", value: "stringval"],
        [type: "DATE", value: "2023-01-01"],
        [type: "DATEV2", value: "2023-01-02"],
        [type: "DATETIME", value: "2023-01-03 10:11:12"],
        [type: "DATETIMEV2(3)", value: "2023-01-04 10:11:12.123"],
        [type: "BOOLEAN", value: "1"]
    ]

    def colNames = new String[totalColumns]
    def colTypes = new String[totalColumns]
    def colTypeIdx = new int[totalColumns]  // index into baseTypes
    colNames[0] = "k0"
    colTypes[0] = "BIGINT"
    for (int i = 1; i < totalColumns; i++) {
        def typeIndex = i % baseTypes.size()
        def base = baseTypes[typeIndex]
        colNames[i] = String.format("c%04d", i)
        colTypes[i] = base.type
        colTypeIdx[i] = typeIndex
    }

    // Generate value for a column based on row number
    def generateValue = { int col, long rowNum ->
        def typeIndex = colTypeIdx[col]
        def v = rowNum + col  // make it vary by both row and column
        switch (typeIndex) {
            case 0:  // TINYINT
                return String.valueOf((v % 127) + 1)
            case 1:  // SMALLINT
                return String.valueOf((v % 32000) + 1)
            case 2:  // INT
                return String.valueOf(v)
            case 3:  // BIGINT
                return String.valueOf(v)
            case 4:  // LARGEINT
                return String.valueOf(v * 1000000000L)
            case 5:  // FLOAT
                return String.format("%.2f", (v % 10000) / 100.0)
            case 6:  // DOUBLE
                return String.format("%.4f", (v % 1000000) / 10000.0)
            case 7:  // DECIMALV3(18,6)
                return String.format("%d.%06d", v % 1000000, v % 1000000)
            case 8:  // CHAR(10)
                return String.format("c%08d", v % 100000000)
            case 9:  // VARCHAR(20)
                return String.format("v%010d", v % 10000000000L)
            case 10: // STRING
                return String.format("s%012d", v)
            case 11: // DATE
                return String.format("2020-%02d-%02d", (v % 12) + 1, (v % 28) + 1)
            case 12: // DATEV2
                return String.format("2021-%02d-%02d", (v % 12) + 1, (v % 28) + 1)
            case 13: // DATETIME
                return String.format("2022-%02d-%02d %02d:%02d:%02d", (v % 12) + 1, (v % 28) + 1, v % 24, v % 60, v % 60)
            case 14: // DATETIMEV2(3)
                return String.format("2023-%02d-%02d %02d:%02d:%02d.%03d", (v % 12) + 1, (v % 28) + 1, v % 24, v % 60, v % 60, v % 1000)
            case 15: // BOOLEAN
                return String.valueOf(v % 2)
            default:
                return String.valueOf(v)
        }
    }

    def columnsDdl = {
        def sb = new StringBuilder()
        sb.append("`").append(colNames[0]).append("` ").append(colTypes[0]).append(" NOT NULL")
        for (int i = 1; i < totalColumns; i++) {
            sb.append(",\n  `").append(colNames[i]).append("` ").append(colTypes[i]).append(" NULL")
        }
        return sb.toString()
    }.call()

    def prepareDataFiles = { int sparsePercent ->
        def cacheRoot = "${context.config.cacheDataPath}/compaction_sparse_wide_table"
        def dir = new File("${cacheRoot}/sparse_${sparsePercent}")
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("create dir failed: ${dir}")
        }
        def metaFile = new File(dir, "READY")
        def dataFiles = (0..<loadTimes).collect { idx ->
            new File(dir, String.format("wide_%d_%d_%03d.csv", totalColumns, rowsPerLoad, idx))
        }
        if (enableCache) {
            if (metaFile.exists() && dataFiles.every { it.exists() }) {
                logger.info("Reuse cached data files: ${dir.absolutePath}")
                return dataFiles.collect { it.absolutePath }
            }
        }

        if (cleanIncompleteCache) {
            dataFiles.each { file ->
                if (file.exists() && !file.delete()) {
                    logger.info("Failed to delete cached file: ${file.absolutePath}")
                }
            }
            dir.listFiles(new FilenameFilter() {
                @Override
                boolean accept(File d, String name) {
                    return name.endsWith(".tmp")
                }
            })?.each { file ->
                if (file.exists() && !file.delete()) {
                    logger.info("Failed to delete tmp file: ${file.absolutePath}")
                }
            }
            if (metaFile.exists() && !metaFile.delete()) {
                logger.info("Failed to delete cache marker: ${metaFile.absolutePath}")
            }
        }

        int estimatedLen = totalColumns * 6
        def generateOne = { File dataFile, int idx ->
            if (dataFile.exists()) {
                return
            }
            logger.info("Generate sparse data file: ${dataFile.absolutePath}, sparsity=${sparsePercent}%, part=${idx}")
            def tmpFile = new File(dir, String.format("wide_%d_%d_%03d.tmp", totalColumns, rowsPerLoad, idx))
            if (tmpFile.exists()) {
                tmpFile.delete()
            }
            def rand = new Random(12345L + sparsePercent * 1000L + idx)
            long baseKey = (long) idx * (long) rowsPerLoad
            tmpFile.withOutputStream { os ->
                def writer = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.US_ASCII), 1024 * 1024)
                def line = new StringBuilder(estimatedLen)
                for (int row = 0; row < rowsPerLoad; row++) {
                    long rowNum = baseKey + row + 1
                    line.setLength(0)
                    line.append(rowNum)
                    for (int col = 1; col < totalColumns; col++) {
                        line.append(columnSeparator)
                        if (rand.nextInt(100) < sparsePercent) {
                            line.append(generateValue(col, rowNum))
                        } else {
                            line.append(nullToken)
                        }
                    }
                    writer.write(line.toString())
                    writer.newLine()
                }
                writer.flush()
                writer.close()
            }
            if (dataFile.exists() && !dataFile.delete()) {
                throw new IOException("delete old data file failed: ${dataFile.absolutePath}")
            }
            if (!tmpFile.renameTo(dataFile)) {
                Files.move(tmpFile.toPath(), dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        }

        def genThreads = Math.min(genParallelism, loadTimes)
        def executor = Executors.newFixedThreadPool(genThreads)
        def futures = []
        try {
            dataFiles.eachWithIndex { File dataFile, int idx ->
                futures << executor.submit({
                    generateOne(dataFile, idx)
                })
            }
            futures.each { it.get() }
        } finally {
            executor.shutdown()
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        }

        metaFile.text = "rows=${rowsPerLoad}\ncols=${totalColumns}\nfiles=${loadTimes}\nsparsity_percent=${sparsePercent}\nseparator=,\nnull=${nullToken}\n"
        return dataFiles.collect { it.absolutePath }
    }

    def createTable = { String tableName, String keyType ->
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              ${columnsDdl}
            ) ENGINE=OLAP
            ${keyType}(`k0`)
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1",
              "disable_auto_compaction" = "true"
            );
        """
    }

    def streamLoadOnce = { String tableName, String filePath ->
        def label = "${tableName}_${UUID.randomUUID().toString().replaceAll('-', '')}"
        streamLoad {
            table tableName
            set 'label', label
            set 'column_separator', columnSeparator
            set 'null', nullToken
            file filePath
            time 600000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(rowsPerLoad, json.NumberTotalRows)
                assertEquals(rowsPerLoad, json.NumberLoadedRows)
            }
        }
    }

    def loadData = { String tableName, List<String> filePaths ->
        def executor = Executors.newFixedThreadPool(parallelism)
        def futures = []
        try {
            filePaths.each { filePath ->
                futures << executor.submit({
                    streamLoadOnce(tableName, filePath)
                })
            }
            futures.each { it.get() }
        } finally {
            executor.shutdown()
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        }
        sql "sync"
    }

    def tableConfigs = [
        [suffix: "dup", keyType: "DUPLICATE KEY"],
        [suffix: "uniq", keyType: "UNIQUE KEY"]
    ]

    // Get BE info
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    // Get tablet info for a table
    def getTabletInfo = { String tableName ->
        def tablets = sql_return_maparray """show tablets from ${tableName}"""
        if (tablets.size() != 1) {
            throw new Exception("Expected 1 tablet for table ${tableName}, but got ${tablets.size()}")
        }
        def tablet = tablets[0]
        def beHost = backendId_to_backendIP["${tablet.BackendId}"]
        def bePort = backendId_to_backendHttpPort["${tablet.BackendId}"]
        return [tabletId: tablet.TabletId, beHost: beHost, bePort: bePort, backendId: tablet.BackendId]
    }

    // Get rowset count for a tablet
    def getRowsetCount = { String beHost, String bePort, String tabletId ->
        def (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
        if (code != 0) {
            throw new Exception("Failed to get tablet status: code=${code}, err=${err}")
        }
        def tabletStatus = parseJson(out.trim())
        return tabletStatus.rowsets instanceof List ? ((List) tabletStatus.rowsets).size() : 0
    }

    // Parse compaction cost from BE log file using grep
    // Log format: "finish CloudCumulativeCompaction, tablet_id=xxx, cost=xxxms, range=[x-x]"
    // Or: "succeed to do cumulative compaction ... tablet=xxx ... elapsed time=xxxs"
    def parseCompactionCostFromLog = { String tabletId, long afterTimestamp ->
        if (beLogFilePath == null) {
            return null
        }
        def logFile = new File(beLogFilePath)
        if (!logFile.exists()) {
            logger.warn("BE log file not found: ${beLogFilePath}")
            return null
        }

        def costMs = null
        def pattern1 = ~/cost=(\d+)ms/
        def pattern2 = ~/elapsed time=([0-9.]+)s/

        try {
            // Use grep to find CumulativeCompaction logs for this tablet
            def cmd = ["bash", "-c", "grep -E 'CumulativeCompaction.*tablet_id=${tabletId}|cumulative compaction.*tablet=${tabletId}' ${beLogFilePath} | tail -1"]
            def proc = cmd.execute()
            proc.waitFor()
            def line = proc.text.trim()

            if (line) {
                // Try to extract cost=xxxms
                def matcher1 = pattern1.matcher(line)
                if (matcher1.find()) {
                    costMs = Long.parseLong(matcher1.group(1))
                    logger.info("Found compaction cost in log: tablet_id=${tabletId}, cost=${costMs}ms, log=${line}")
                } else {
                    // Try to extract elapsed time=xxxs
                    def matcher2 = pattern2.matcher(line)
                    if (matcher2.find()) {
                        def elapsedSec = Double.parseDouble(matcher2.group(1))
                        costMs = (long)(elapsedSec * 1000)
                        logger.info("Found compaction elapsed time in log: tablet_id=${tabletId}, elapsed=${elapsedSec}s")
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse BE log: ${e.message}")
        }
        return costMs
    }

    // Get BE pid from pid file
    def getBePid = {
        def pidFile = new File(bePidFile)
        if (!pidFile.exists()) {
            logger.warn("BE pid file not found: ${bePidFile}")
            return null
        }
        return pidFile.text.trim()
    }

    // Check if cumu compaction thread is running using top -H
    def isCumuThreadRunning = { String bePid ->
        try {
            // Use top -H -b -n 1 to get thread info, look for Cumu thread in COMMAND column
            def cmd = ["bash", "-c", "top -H -b -n 1 -p ${bePid} 2>/dev/null | grep -i 'Cumu'"]
            def proc = cmd.execute()
            proc.waitFor()
            def output = proc.text.trim()
            if (output.length() > 0) {
                logger.info("Detected Cumu thread: ${output}")
            }
            return output.length() > 0
        } catch (Exception e) {
            logger.warn("Failed to check cumu thread: ${e.message}")
            return false
        }
    }

    // Capture flame graph for BE process
    // perf.sh outputs: "flame graph generated: VM-10-11-centos-20260114203243.svg"
    def captureFlameGraph = { String bePid, String newName ->
        if (!enableFlameGraph) {
            return null
        }
        try {
            logger.info("Capturing flame graph for BE pid=${bePid}...")
            // Use sudo -n (non-interactive) to avoid tty requirement
            // Use timeout to avoid hanging forever
            // IMPORTANT: redirect stdin from /dev/null to prevent pipeline deadlock
            def cmd = "cd ${flameGraphDir} && timeout 120 sudo -n sh ./perf.sh ${bePid} </dev/null 2>&1"
            def proc = ["bash", "-c", cmd].execute()

            // Read output with timeout to avoid blocking
            def output = new StringBuilder()
            def reader = new BufferedReader(new InputStreamReader(proc.inputStream))
            def line
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n")
                logger.info("perf.sh: ${line}")
            }
            reader.close()
            proc.waitFor()
            logger.info("perf.sh completed with exit code: ${proc.exitValue()}")

            // Parse generated file name from output: "flame graph generated: xxx.svg"
            def matcher = (output =~ /flame graph generated:\s*(\S+\.svg)/)
            if (matcher.find()) {
                def generatedFile = matcher.group(1)
                def srcPath = "${flameGraphDir}/${generatedFile}"
                def newFileName = "${newName}.svg"
                def destPath = "${flameGraphDir}/${newFileName}"

                // Use sudo -n to rename since file is created by root
                def mvCmd = "sudo -n mv '${srcPath}' '${destPath}'"
                def mvProc = ["bash", "-c", mvCmd].execute()
                mvProc.text  // consume output
                mvProc.waitFor()

                if (new File(destPath).exists()) {
                    logger.info("Flame graph renamed: ${generatedFile} -> ${newFileName}")
                    return destPath
                } else if (new File(srcPath).exists()) {
                    logger.warn("Rename failed, keeping original: ${srcPath}")
                    return srcPath
                } else {
                    logger.warn("Flame graph file not found")
                    return null
                }
            } else {
                logger.warn("Could not parse flame graph file name from output")
                return null
            }
        } catch (Exception e) {
            logger.warn("Failed to capture flame graph: ${e.message}")
            return null
        }
    }

    // Trigger and wait for cumulative compaction to complete, return elapsed time
    def triggerAndWaitCumuCompaction = { String tableName, Map tabletInfo ->
        def beHost = tabletInfo.beHost
        def bePort = tabletInfo.bePort
        def tabletId = tabletInfo.tabletId

        // Get tablet status before compaction
        def (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
        if (code != 0) {
            throw new Exception("Failed to get tablet status before compaction: ${err}")
        }
        def statusBefore = parseJson(out.trim())
        def lastCumuSuccessTime = statusBefore["last cumulative success time"]

        // Get rowset count before compaction
        def rowsetCountBefore = statusBefore.rowsets instanceof List ? ((List) statusBefore.rowsets).size() : 0
        logger.info("Table ${tableName}: Before compaction, rowset_count=${rowsetCountBefore}, tablet_id=${tabletId}")

        // Record start time
        def startTime = System.currentTimeMillis()

        // Trigger cumulative compaction
        (code, out, err) = be_run_cumulative_compaction(beHost, bePort, tabletId)
        if (code != 0) {
            throw new Exception("Failed to trigger cumulative compaction: ${err}")
        }
        def triggerResult = parseJson(out.trim())
        if (triggerResult.status.toLowerCase() != "success" && triggerResult.status.toLowerCase() != "already_exist") {
            throw new Exception("Trigger cumulative compaction failed: ${triggerResult.status}")
        }
        logger.info("Table ${tableName}: Triggered cumulative compaction, tablet_id=${tabletId}")

        // Capture flame graph if enabled
        def flameGraphPath = null
        if (enableFlameGraph) {
            def bePid = getBePid()
            if (bePid != null) {
                // Wait for cumu thread to start (poll for up to 30 seconds)
                logger.info("Table ${tableName}: Waiting for Cumu thread to start...")
                def cumuThreadStarted = false
                for (int i = 0; i < 30; i++) {
                    if (isCumuThreadRunning(bePid)) {
                        cumuThreadStarted = true
                        logger.info("Table ${tableName}: Cumu thread detected, capturing flame graph...")
                        break
                    }
                    Thread.sleep(1000)
                }

                if (cumuThreadStarted) {
                    // Generate output name: cumu_<type>_<sparsity>_<tabletId>
                    def outputName = "cumu_${tableName}"
                    flameGraphPath = captureFlameGraph(bePid, outputName)
                } else {
                    logger.warn("Table ${tableName}: Cumu thread not detected within 30s, skipping flame graph")
                }
            }
        }

        // Wait for compaction to complete
        Awaitility.await().atMost(compactionTimeoutSeconds, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until({
            def (c, o, e) = be_get_compaction_status(beHost, bePort, tabletId)
            if (c != 0) {
                return false
            }
            def compactionStatus = parseJson(o.trim())
            if (compactionStatus.run_status) {
                return false  // still running
            }

            // Check if last cumulative success time is updated
            def (c2, o2, e2) = be_show_tablet_status(beHost, bePort, tabletId)
            if (c2 != 0) {
                return false
            }
            def statusAfter = parseJson(o2.trim())
            return statusAfter["last cumulative success time"] != lastCumuSuccessTime
        })

        def endTime = System.currentTimeMillis()
        def clientElapsedMs = endTime - startTime

        // Try to parse precise cost from BE log
        def logCostMs = parseCompactionCostFromLog(tabletId, startTime)

        // Get rowset count after compaction
        (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
        def statusAfter = parseJson(out.trim())
        def rowsetCountAfter = statusAfter.rowsets instanceof List ? ((List) statusAfter.rowsets).size() : 0

        if (logCostMs != null) {
            logger.info("Table ${tableName}: After compaction, rowset_count=${rowsetCountAfter}, " +
                       "log_cost=${logCostMs}ms, client_elapsed=${clientElapsedMs}ms")
        } else {
            logger.info("Table ${tableName}: After compaction, rowset_count=${rowsetCountAfter}, " +
                       "client_elapsed=${clientElapsedMs}ms (log cost not available)")
        }

        if (flameGraphPath != null) {
            logger.info("Table ${tableName}: Flame graph saved to ${flameGraphPath}")
        }

        return [
            clientElapsedMs: clientElapsedMs,
            logCostMs: logCostMs,
            rowsetBefore: rowsetCountBefore,
            rowsetAfter: rowsetCountAfter,
            flameGraphPath: flameGraphPath
        ]
    }

    // ========== TEST MODE: Only test flame graph ==========
    if (testFlameGraphOnly) {
        logger.info("===== FLAME GRAPH TEST MODE =====")
        def pid = testPid ?: getBePid()
        logger.info("Testing flame graph capture for pid: ${pid}")

        if (pid) {
            def result = captureFlameGraph(pid, "test_flame_graph")
            if (result) {
                logger.info("SUCCESS: Flame graph saved to ${result}")
            } else {
                logger.error("FAILED: Could not capture flame graph")
            }
        } else {
            logger.error("FAILED: Could not get BE pid")
        }

        logger.info("===== FLAME GRAPH TEST COMPLETE =====")
        return
    }
    // ========================================================

    // Store all table info for compaction phase
    def allTables = []

    // Phase 1: Create tables and load data
    for (def tableConfig : tableConfigs) {
        for (int sparsePercent : sparsityLevels) {
            def tableName = "compaction_sparse_wide_${tableConfig.suffix}_${sparsePercent}"
            try {
                def dataFiles = prepareDataFiles(sparsePercent)
                createTable(tableName, tableConfig.keyType)
                loadData(tableName, dataFiles)
                allTables.add([
                    tableName: tableName,
                    keyType: tableConfig.suffix,
                    sparsePercent: sparsePercent
                ])
                logger.info("Created and loaded table: ${tableName}")
            } catch (Exception e) {
                logger.error("Failed to create/load table ${tableName}: ${e.message}")
                throw e
            }
        }
    }

    // Phase 2: Trigger compaction serially and record timing
    if (triggerCompaction) {
        logger.info("=" * 80)
        logger.info("Starting compaction phase for ${allTables.size()} tables")
        logger.info("=" * 80)

        def compactionResults = []

        for (def tableInfo : allTables) {
            def tableName = tableInfo.tableName

            try {
                // Get tablet info
                def tabletInfo = getTabletInfo(tableName)
                logger.info("Table ${tableName}: tablet_id=${tabletInfo.tabletId}, be=${tabletInfo.beHost}:${tabletInfo.bePort}")

                // Verify rowset count (should be loadTimes + 1 due to initial empty rowset)
                def rowsetCount = getRowsetCount(tabletInfo.beHost, tabletInfo.bePort, tabletInfo.tabletId)
                logger.info("Table ${tableName}: Current rowset count = ${rowsetCount} (expected ~${loadTimes + 1})")

                // Trigger compaction and wait for completion
                def result = triggerAndWaitCumuCompaction(tableName, tabletInfo)

                compactionResults.add([
                    tableName: tableName,
                    keyType: tableInfo.keyType,
                    sparsePercent: tableInfo.sparsePercent,
                    logCostMs: result.logCostMs,
                    clientElapsedMs: result.clientElapsedMs,
                    rowsetBefore: result.rowsetBefore,
                    rowsetAfter: result.rowsetAfter,
                    tabletId: tabletInfo.tabletId,
                    flameGraphPath: result.flameGraphPath
                ])

                def costDisplay = result.logCostMs != null ? "${result.logCostMs}ms (from log)" : "${result.clientElapsedMs}ms (client)"
                logger.info("Table ${tableName}: Compaction completed, cost=${costDisplay}")

            } catch (Exception e) {
                logger.error("Failed to compact table ${tableName}: ${e.message}")
                compactionResults.add([
                    tableName: tableName,
                    keyType: tableInfo.keyType,
                    sparsePercent: tableInfo.sparsePercent,
                    logCostMs: null,
                    clientElapsedMs: -1,
                    error: e.message
                ])
            }
        }

        // Print final summary
        logger.info("=" * 120)
        logger.info("COMPACTION RESULTS SUMMARY")
        logger.info("=" * 120)
        logger.info(String.format("%-42s %-6s %-8s %-12s %-12s %-8s %-8s %-15s",
            "Table Name", "Type", "Sparse%", "LogCost(ms)", "ClientMs", "Before", "After", "TabletId"))
        logger.info("-" * 120)

        for (def result : compactionResults) {
            if (result.error) {
                logger.info(String.format("%-42s %-6s %-8d %-12s %-12s ERROR: %s",
                    result.tableName, result.keyType, result.sparsePercent, "FAILED", "-", result.error))
            } else {
                def logCostStr = result.logCostMs != null ? String.valueOf(result.logCostMs) : "N/A"
                logger.info(String.format("%-42s %-6s %-8d %-12s %-12d %-8d %-8d %-15s",
                    result.tableName, result.keyType, result.sparsePercent,
                    logCostStr, result.clientElapsedMs, result.rowsetBefore, result.rowsetAfter, result.tabletId))
            }
        }
        logger.info("=" * 120)

        // Print comparison by sparsity level
        logger.info("")
        logger.info("COMPACTION TIME BY SPARSITY LEVEL (using log cost if available, otherwise client elapsed)")
        logger.info("-" * 100)
        for (int sparsePercent : sparsityLevels) {
            def dupResult = compactionResults.find { it.keyType == "dup" && it.sparsePercent == sparsePercent }
            def uniqResult = compactionResults.find { it.keyType == "uniq" && it.sparsePercent == sparsePercent }

            def dupCostMs = dupResult?.logCostMs ?: dupResult?.clientElapsedMs ?: -1
            def uniqCostMs = uniqResult?.logCostMs ?: uniqResult?.clientElapsedMs ?: -1

            def dupStr = dupCostMs > 0 ? String.format("%8dms", dupCostMs) : "   N/A   "
            def uniqStr = uniqCostMs > 0 ? String.format("%8dms", uniqCostMs) : "   N/A   "

            logger.info(String.format("Sparsity %3d%%: DUP = %s, UNIQ = %s", sparsePercent, dupStr, uniqStr))
        }
        logger.info("-" * 100)

        // Print tablet IDs for manual log grep if needed
        logger.info("")
        logger.info("Tablet IDs (for manual log grep if needed):")
        logger.info("  grep 'finish Cloud.*Compaction.*tablet_id=<id>.*cost=' be.INFO")
        for (def result : compactionResults) {
            if (!result.error) {
                logger.info("  ${result.tableName}: tablet_id=${result.tabletId}")
            }
        }

        // Print flame graph paths if any
        def flameGraphs = compactionResults.findAll { it.flameGraphPath != null }
        if (flameGraphs.size() > 0) {
            logger.info("")
            logger.info("Flame Graphs:")
            for (def result : flameGraphs) {
                logger.info("  ${result.tableName}: ${result.flameGraphPath}")
            }
        }
    }

    // Cleanup
    if (dropTablesAfterLoad) {
        for (def tableInfo : allTables) {
            try_sql("DROP TABLE IF EXISTS ${tableInfo.tableName} FORCE")
        }
    }
}
