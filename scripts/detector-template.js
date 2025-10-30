function detect({ op }) {
  // op.operation contains the PLC operation data
  // op.did - DID identifier
  // op.cid - Content ID
  // op.createdAt - Timestamp

  console.log(op.did)
  
  const labels = [];
  
  // Add your detection logic here
  // Return array of label strings
  // Return empty array [] for no match
  
  return labels;
}
