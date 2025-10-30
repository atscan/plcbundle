function detect({ op }) {
  // op.operation contains the PLC operation data
  // op.did - DID identifier
  // op.cid - Content ID
  // op.createdAt - Timestamp
  
  const labels = [];
  
  // Add your detection logic here
  // Return array of label strings
  // Return empty array [] for no match

    //console.log(JSON.stringify(op))

  if (op.did.match(/^did:plc:aaa/)) {
    labels.push('test')
  }
  
  return labels;
}
