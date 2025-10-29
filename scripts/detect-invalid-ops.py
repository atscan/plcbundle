#!/usr/bin/env python3
"""
detect-invalid-ops.py - Detect invalid PLC operations (optimized for scale)

Validates PLC operations according to did:plc specification v0.2.1
Optimized for processing millions of operations efficiently.
"""

import sys
import json
import csv
import base64
import time
from typing import List, Optional, Dict, Any
from collections import defaultdict

try:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

# Performance: Pre-compile valid field sets
VALID_TOP_FIELDS = frozenset({'did', 'operation', 'cid', 'nullified', 'createdAt'})
VALID_PLC_OP_FIELDS = frozenset({'type', 'rotationKeys', 'verificationMethods', 
                                  'alsoKnownAs', 'services', 'prev', 'sig'})
VALID_LEGACY_FIELDS = frozenset({'type', 'signingKey', 'recoveryKey', 
                                  'handle', 'service', 'prev', 'sig'})
VALID_TOMBSTONE_FIELDS = frozenset({'type', 'prev', 'sig'})
VALID_BASE64URL_CHARS = frozenset('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_')

# EC curve orders for High-S check
P256_ORDER = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551
K256_ORDER = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
P256_HALF_ORDER = P256_ORDER // 2
K256_HALF_ORDER = K256_ORDER // 2


class OperationValidator:
    def __init__(self, skip_high_s=False):
        self.op_count = 0
        self.invalid_count = 0
        self.skip_high_s = skip_high_s or not HAS_CRYPTO
        self.start_time = time.time()
        self.last_progress_time = time.time()
        self.reason_counts = defaultdict(int)
        
    def log_progress(self, message: str):
        print(f"[PROGRESS] {message}", file=sys.stderr, flush=True)
    
    def check_extra_fields(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check for extra fields - optimized"""
        # Fast path: check top-level first (most common)
        extra_top = set(operation.keys()) - VALID_TOP_FIELDS
        if extra_top:
            return f"extra-top-level:{','.join(sorted(extra_top))}"
        
        op_data = operation.get('operation', {})
        op_type = op_data.get('type')
        
        # Fast path: direct field set comparison
        op_keys = set(op_data.keys())
        
        if op_type == 'plc_operation':
            extra = op_keys - VALID_PLC_OP_FIELDS
            if extra:
                return f"extra-plc-operation:{','.join(sorted(extra))}"
            
            # Only check VM structure if we have verificationMethods
            vm = op_data.get('verificationMethods')
            if vm:
                for key, value in vm.items():
                    if isinstance(value, dict):
                        return f"invalid-verification-method:{key}"
                        
        elif op_type == 'create':
            extra = op_keys - VALID_LEGACY_FIELDS
            if extra:
                return f"extra-legacy-create:{','.join(sorted(extra))}"
                
        elif op_type == 'plc_tombstone':
            extra = op_keys - VALID_TOMBSTONE_FIELDS
            if extra:
                return f"extra-tombstone:{','.join(sorted(extra))}"
        
        return None
    
    def check_signature_encoding(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check signature encoding - optimized"""
        sig = operation.get('operation', {}).get('sig')
        
        if not sig:
            return None
        
        # Fast checks first
        sig_len = len(sig)
        if sig_len != 86:
            return f"sig-wrong-length:{sig_len}"
        
        if '=' in sig:
            return "sig-has-padding"
        
        # Check valid chars (using set for O(1) lookup)
        if not set(sig).issubset(VALID_BASE64URL_CHARS):
            return "sig-invalid-chars"
        
        return None
    
    def check_high_s_signature(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check for High-S signatures - optimized"""
        if self.skip_high_s:
            return None
        
        sig = operation.get('operation', {}).get('sig')
        if not sig:
            return None
        
        try:
            # Fast decode with minimal validation
            sig_bytes = base64.urlsafe_b64decode(sig + '==')
            
            if len(sig_bytes) != 64:
                return None  # Already caught by encoding check
            
            # Extract s value (second 32 bytes)
            s = int.from_bytes(sig_bytes[32:], 'big')
            
            # Check against both curve half-orders
            if s > P256_HALF_ORDER or s > K256_HALF_ORDER:
                return "high-s-signature"
            
        except Exception:
            return None  # Encoding errors caught elsewhere
        
        return None
    
    def check_duplicate_rotation_keys(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check for duplicate rotation keys - optimized"""
        op_data = operation.get('operation', {})
        
        if op_data.get('type') != 'plc_operation':
            return None
        
        keys = op_data.get('rotationKeys')
        if not keys:
            return None
        
        keys_len = len(keys)
        unique_len = len(set(keys))
        
        if unique_len < keys_len:
            return f"duplicate-rotation-keys:{keys_len}-total-{unique_len}-unique"
        
        return None
    
    def check_rotation_keys_count(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check rotation keys count - optimized"""
        op_data = operation.get('operation', {})
        
        if op_data.get('type') != 'plc_operation':
            return None
        
        keys = op_data.get('rotationKeys')
        if not keys:
            return None
        
        count = len(keys)
        if count < 1 or count > 5:
            return f"invalid-rotation-keys-count:{count}"
        
        return None
    
    def check_verification_methods_count(self, operation: Dict[str, Any]) -> Optional[str]:
        """Check verification methods count - optimized"""
        op_data = operation.get('operation', {})
        
        if op_data.get('type') != 'plc_operation':
            return None
        
        vm = op_data.get('verificationMethods')
        if not vm:
            return None
        
        count = len(vm)
        if count > 10:
            return f"too-many-verification-methods:{count}"
        
        return None
    
    def validate_operation(self, operation: Dict[str, Any]) -> List[str]:
        """Run all validations - optimized hot path"""
        reasons = []
        
        # Inline checks for performance (avoid function call overhead)
        result = self.check_extra_fields(operation)
        if result:
            reasons.append(result)
        
        result = self.check_signature_encoding(operation)
        if result:
            reasons.append(result)
        
        if not self.skip_high_s:
            result = self.check_high_s_signature(operation)
            if result:
                reasons.append(result)
        
        result = self.check_duplicate_rotation_keys(operation)
        if result:
            reasons.append(result)
        
        result = self.check_rotation_keys_count(operation)
        if result:
            reasons.append(result)
        
        result = self.check_verification_methods_count(operation)
        if result:
            reasons.append(result)
        
        return reasons
    
    def process_stream(self):
        """Process operations from stdin - optimized"""
        writer = csv.writer(sys.stdout, lineterminator='\n')
        writer.writerow(['bundle', 'position', 'reason', 'opRaw'])
        
        # Buffer for batch writing (improves I/O performance)
        buffer = []
        buffer_size = 100
        
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            
            self.op_count += 1
            
            # Calculate bundle and position
            bundle = (self.op_count - 1) // 10000 + 1
            position = (self.op_count - 1) % 10000
            
            try:
                operation = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            # Validate
            reasons = self.validate_operation(operation)
            
            if reasons:
                self.invalid_count += 1
                reason_str = '|'.join(reasons)
                
                # Track reason statistics
                for reason in reasons:
                    self.reason_counts[reason] += 1
                
                # Compact JSON (no spaces)
                op_raw = json.dumps(operation, separators=(',', ':'))
                
                buffer.append([bundle, position, reason_str, op_raw])
                
                # Batch write for performance
                if len(buffer) >= buffer_size:
                    writer.writerows(buffer)
                    buffer.clear()
            
            # Progress (throttled to once per second)
            if self.op_count % 10000 == 0:
                current_time = time.time()
                if current_time - self.last_progress_time >= 1.0:
                    elapsed = current_time - self.start_time
                    rate = self.op_count / elapsed
                    self.log_progress(
                        f"Processed {self.op_count:,} ops "
                        f"({rate:,.0f} ops/sec) | "
                        f"Invalid: {self.invalid_count:,}"
                    )
                    self.last_progress_time = current_time
        
        # Flush remaining buffer
        if buffer:
            writer.writerows(buffer)
        
        # Final summary
        elapsed = time.time() - self.start_time
        if self.op_count > 0:
            percentage = (self.invalid_count / self.op_count) * 100
            rate = self.op_count / elapsed
            self.log_progress("=" * 60)
            self.log_progress(f"Complete: {self.op_count:,} operations in {elapsed:.1f}s")
            self.log_progress(f"Rate: {rate:,.0f} ops/sec")
            self.log_progress(f"Invalid: {self.invalid_count:,} ({percentage:.4f}%)")
            
            # Show top invalid reasons
            if self.reason_counts:
                self.log_progress("\nTop invalid reasons:")
                sorted_reasons = sorted(
                    self.reason_counts.items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]
                for reason, count in sorted_reasons:
                    self.log_progress(f"  {reason}: {count:,}")
        else:
            self.log_progress("No operations processed")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Detect invalid PLC operations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  plcbundle export -count 10000 | %(prog)s > invalid.csv
  plcbundle backfill | %(prog)s > invalid.csv
  
  # Skip High-S check for speed
  plcbundle backfill | %(prog)s --skip-high-s > invalid.csv

Performance:
  ~50,000-100,000 ops/sec on modern hardware
  ~1-2 hours for 10 million operations
  Memory usage: <100MB
        '''
    )
    
    parser.add_argument(
        '--skip-high-s',
        action='store_true',
        help='Skip High-S validation (faster)'
    )
    
    args = parser.parse_args()
    
    if not HAS_CRYPTO and not args.skip_high_s:
        print("[WARNING] cryptography not installed. High-S validation disabled.", file=sys.stderr)
        print("[WARNING] Install with: pip install cryptography", file=sys.stderr)
        args.skip_high_s = True
    
    validator = OperationValidator(skip_high_s=args.skip_high_s)
    
    try:
        validator.process_stream()
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]", file=sys.stderr)
        sys.exit(1)
    except BrokenPipeError:
        sys.exit(0)


if __name__ == '__main__':
    main()
