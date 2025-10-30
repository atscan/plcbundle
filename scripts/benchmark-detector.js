#!/usr/bin/env bun

// User's detect function
function detect({ op }) {
  const labels = [];
  
  if (op.did.startsWith('did:plc:aa')) {
    labels.push('test')
  }

  console.log(op.operation.sig)
  
  return labels;
}

// ==========================================
// Pure Bun bundle processor with native zstd
// ==========================================

const BUNDLE_DIR = process.argv[2] || './';
const START_BUNDLE = parseInt(process.argv[3]) || 1;
const END_BUNDLE = parseInt(process.argv[4]) || 100;

console.error(`Processing bundles ${START_BUNDLE}-${END_BUNDLE} from ${BUNDLE_DIR}`);
console.error('');

// CSV header
console.log('bundle,position,cid,size,confidence,labels');

let totalOps = 0;
let matchCount = 0;
let totalBytes = 0;
let matchedBytes = 0;

const startTime = Date.now();

for (let bundleNum = START_BUNDLE; bundleNum <= END_BUNDLE; bundleNum++) {
  const bundleFile = `${BUNDLE_DIR}/${bundleNum.toString().padStart(6, '0')}.jsonl.zst`;
  
  try {
    // Read compressed bundle
    const compressed = await Bun.file(bundleFile).arrayBuffer();
    
    // Decompress using native Bun zstd (FAST!)
    const decompressed = Bun.zstdDecompressSync(compressed);
    
    // Convert to text
    const text = new TextDecoder().decode(decompressed);
    
    const lines = text.split('\n').filter(line => line.trim());
    
    for (let position = 0; position < lines.length; position++) {
      const line = lines[position];
      if (!line) continue;
      
      totalOps++;
      const opSize = line.length;
      totalBytes += opSize;
      
      try {
        const op = JSON.parse(line);
        const labels = detect({ op });
        
        if (labels && labels.length > 0) {
          matchCount++;
          matchedBytes += opSize;
          
          // Extract last 4 chars of CID
          const cidShort = op.cid.slice(-4);
          
          console.log(
            `${bundleNum},${position},${cidShort},${opSize},0.95,${labels.join(';')}`
          );
        }
      } catch (err) {
        console.error(`Error parsing operation: ${err.message}`);
      }
    }
    
    // Progress
    if (bundleNum % 10 === 0) {
      const elapsed = (Date.now() - startTime) / 1000;
      const opsPerSec = (totalOps / elapsed).toFixed(0);
      console.error(`Processed ${bundleNum}/${END_BUNDLE} bundles | ${totalOps} ops | ${opsPerSec} ops/sec\r`);
    }
    
  } catch (err) {
    console.error(`\nError loading bundle ${bundleNum}: ${err.message}`);
  }
}

const elapsed = (Date.now() - startTime) / 1000;

// Stats
console.error('\n');
console.error('✓ Detection complete');
console.error(`  Total operations:   ${totalOps}`);
console.error(`  Matches found:      ${matchCount} (${(matchCount/totalOps*100).toFixed(2)}%)`);
console.error(`  Total size:         ${(totalBytes / 1e6).toFixed(1)} MB`);
console.error(`  Matched size:       ${(matchedBytes / 1e6).toFixed(1)} MB (${(matchedBytes/totalBytes*100).toFixed(2)}%)`);
console.error('');
console.error(`  Time elapsed:       ${elapsed.toFixed(2)}s`);
console.error(`  Throughput:         ${(totalOps / elapsed).toFixed(0)} ops/sec`);
console.error(`  Speed:              ${(totalBytes / elapsed / 1e6).toFixed(1)} MB/sec`);
