package bundleindex

// ChainVerificationResult contains the result of chain verification
type ChainVerificationResult struct {
	Valid           bool
	ChainLength     int
	BrokenAt        int
	Error           string
	VerifiedBundles []int
}

// VerificationResult contains the result of bundle verification
type VerificationResult struct {
	BundleNumber int
	Valid        bool
	HashMatch    bool
	FileExists   bool
	Error        error
	LocalHash    string
	ExpectedHash string
}
