const { exec } = require('child_process');
const util = require('util');
const fs = require('fs').promises;
const path = require('path');

const execAsync = util.promisify(exec);

// --- Configuration file save path ---
// Files will be saved in your system's "Documents" folder, you can modify to any path you prefer
const resultsDir = path.join(require('os').homedir(), 'Documents', 'AwsScriptLogs');
const resultsFilePath = path.join(resultsDir, `execution_results_${Date.now()}.json`);
const failedOnlyFilePath = path.join(resultsDir, `failed_requests_${Date.now()}.json`);
// -----------------------

// Users and data (keep as is)
const users = ["sterna.paramita@traveloka.com"];
const data = [
  {hotelId: "20042989", currency: "IDR", amount: 120350}
];

const totalUsers = users.length;
const totalHotelIds = data.length;
const totalRequests = totalUsers * totalHotelIds;

console.log(`Total requests to be made: ${totalRequests}`);
console.log(`Results will be saved to: ${resultsDir}\n`);

async function executeAwsCommand(command, requestIndex, user, hotelId) {
  const startTime = Date.now();
  try {
    const { stdout, stderr } = await execAsync(command);
    const duration = Date.now() - startTime;
    return {
      success: true,
      index: requestIndex,
      user,
      hotelId,
      durationMs: duration,
      stdout: stdout.substring(0, 500), // Truncate output to avoid excessively large files
      stderr: stderr || null
    };
  } catch (error) {
    const duration = Date.now() - startTime;
    return {
      success: false,
      index: requestIndex,
      user,
      hotelId,
      durationMs: duration,
      error: error.message,
      stderr: error.stderr ? error.stderr.toString().substring(0, 500) : null
    };
  }
}

async function saveResultsToFile(allResults) {
  try {
    // Ensure log directory exists
    await fs.mkdir(resultsDir, { recursive: true });

    // 1. Save complete detailed results with all requests
    const fullResults = {
      summary: {
        total: allResults.length,
        successful: allResults.filter(r => r.success).length,
        failed: allResults.filter(r => !r.success).length,
        timestamp: new Date().toISOString()
      },
      allRequests: allResults
    };
    await fs.writeFile(resultsFilePath, JSON.stringify(fullResults, null, 2));
    console.log(`\n✅ Full detailed results saved to:\n   ${resultsFilePath}`);

    // 2. Save a separate file containing only failed requests for easy retry
    const failedRequests = allResults.filter(r => !r.success);
    if (failedRequests.length > 0) {
      const failedSummary = {
        note: "This file contains only the failed requests for easy retry.",
        summary: {
          totalFailed: failedRequests.length,
          generatedAt: new Date().toISOString()
        },
        failedRequests: failedRequests.map(req => ({
          index: req.index,
          user: req.user,
          hotelId: req.hotelId,
          error: req.error
          // Additional information needed for retry can be added here, such as original payload
        }))
      };
      await fs.writeFile(failedOnlyFilePath, JSON.stringify(failedSummary, null, 2));
      console.log(`✅ Failed requests list saved to:\n   ${failedOnlyFilePath}`);
    }

  } catch (fileError) {
    console.error(`❌ Failed to save results to file: ${fileError.message}`);
    // Even if file save fails, still print summary to console
    printConsoleSummary(allResults);
  }
}

// New function: Save failed records by batch
async function saveFailedBatch(batchResults, batchNumber) {
  try {
    await fs.mkdir(resultsDir, { recursive: true });

    const failedRequests = batchResults.filter(r => !r.success);
    if (failedRequests.length > 0) {
      const failedSummary = {
        note: `Batch ${batchNumber} failed requests`,
        summary: {
          batchNumber: batchNumber,
          totalFailed: failedRequests.length,
          generatedAt: new Date().toISOString()
        },
        failedRequests: failedRequests.map(req => ({
          index: req.index,
          user: req.user,
          hotelId: req.hotelId,
          error: req.error
        }))
      };

      const batchFilePath = path.join(resultsDir, `failed_requests_batch_${batchNumber}.json`);
      await fs.writeFile(batchFilePath, JSON.stringify(failedSummary, null, 2));
      console.log(`  📦 Batch ${batchNumber} failed requests saved to: ${batchFilePath}`);
    }
  } catch (fileError) {
    console.error(`❌ Failed to save batch ${batchNumber} results: ${fileError.message}`);
  }
}

function printConsoleSummary(allResults) {
  const successful = allResults.filter(r => r.success).length;
  const failed = allResults.filter(r => !r.success).length;

  console.log("\n" + "=".repeat(60));
  console.log("EXECUTION SUMMARY (Console)");
  console.log("=".repeat(60));
  console.log(`Total requests: ${allResults.length}`);
  console.log(`✅ Successful: ${successful}`);
  console.log(`❌ Failed: ${failed}`);

  if (failed > 0) {
    console.log("\nFailed request details:");
    allResults.filter(r => !r.success).forEach(r => {
      console.log(`  [${r.index}] User: ${r.user}, Hotel: ${r.hotelId}`);
      console.log(`     Error: ${r.error}`);
    });
  }
  console.log("=".repeat(60));
}

async function processRequestsAsync() {
  const allResults = [];
  let requestCount = 0;
  let batchNumber = 1;
  let batchResults = []; // Used to store current batch request results

  // Prepare commands for all requests
  const requestPromises = [];

  for (const user of users) {
    for (const entry of data) {
      requestCount++;
      const hotelId = entry.hotelId;
      const currency = entry.currency;
      const amount = entry.amount;

      const now = new Date();
      const currentTimestamp = now.getTime();

      const payload = {
        "jsonrpc": "2.0",
        "id": "12345814",
        "source": "astcapi",
        "method": "insertDepositTransactionSync",
        "params": [
          currency,
          {
            "hotelId": hotelId,
            "dateTime": {
              "year": now.getFullYear(),
              "month": now.getMonth() + 1,
              "day": now.getDate(),
              "hour": now.getHours(),
              "minute": now.getMinutes(),
              "second": now.getSeconds(),
              "timestamp": currentTimestamp
            },
            "type": "CREDIT",
            "amount": amount,
            "note": "Manual Adjustment : TERA Deposit Write Off ABO-1058392593",
            "referenceType": "MANUAL_ADJUSTMENT",
            "referenceId": `${user}.${currentTimestamp}`
          }
        ]
      };

      const payloadString = JSON.stringify(payload);
      const curlCommand = `curl --location 'https://astbpbd.ast.tvlk.cloud/tera/hotel/accountReceivable' --header 'Content-Type: application/json' --data-raw '${payloadString.replace(/'/g, "'\"'\"'")}'`;
      const awsCommand = `aws ecs execute-command --cluster ast-bkg-ecs-cluster-a1f360d11f9888fa --task 88e68aa392dc432eb97251809d798a66 --container datadog --interactive --command ${JSON.stringify(`sh -c "${curlCommand.replace(/"/g, '\\"')}"`)}`;

      // Wrap request in Promise but don't execute immediately
      requestPromises.push({
        command: awsCommand,
        index: requestCount,
        user,
        hotelId
      });
    }
  }

  // Execute requests sequentially with 100ms intervals and progress logging every 500 records
  for (let i = 0; i < requestPromises.length; i++) {
    const request = requestPromises[i];

    // Print progress every 500 records
    if (request.index % 500 === 0 || request.index === 1) {
      console.log(`\n📊 PROGRESS: Processing record ${request.index} of ${totalRequests} (${Math.round((request.index / totalRequests) * 100)}%)`);

      // Save current batch error records every 500 records (except the first time)
      if (request.index > 1 && batchResults.length > 0) {
        await saveFailedBatch(batchResults, batchNumber);
        batchResults = []; // Clear current batch results
        batchNumber++; // Increment batch number
      }
    }

    console.log(`\n[${request.index}/${totalRequests}] Processing: User ${request.user}, Hotel ${request.hotelId}`);

    const result = await executeAwsCommand(request.command, request.index, request.user, request.hotelId);
    allResults.push(result);
    batchResults.push(result); // Add to current batch

    const status = result.success ? '✅ Success' : '❌ Failed';
    console.log(`   ${status} (${result.durationMs}ms)`);

    // If not the last request, wait 100ms (reduced from 2000ms)
    if (i < requestPromises.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 100)); // Changed from 2000ms to 100ms
    }
  }

  // Save the last batch (may be less than 500) error records
  if (batchResults.length > 0) {
    await saveFailedBatch(batchResults, batchNumber);
  }

  // Print summary and save to file
  printConsoleSummary(allResults);
  await saveResultsToFile(allResults);

  return allResults;
}

// Main execution flow
(async () => {
  try {
    console.log("🚀 Starting AWS ECS command execution script...");
    await processRequestsAsync();
    console.log("\n✨ Script finished.");
  } catch (globalError) {
    console.error("\n💥 Unexpected script error:", globalError);
    process.exit(1);
  }
})();