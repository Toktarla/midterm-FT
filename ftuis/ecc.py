"""Software model of single-error-correcting memory ECC.

Each 4 data bits are stored as a 7-bit Hamming code. One flipped bit in a
codeword is corrected. Two flipped bits are not reliable, which is the same
limit as SEC Hamming used by memory controllers.

This is not hardware ECC. The CPU still reads ordinary RAM. The redundant
bits live in the application record so the report can show the mechanism
the assignment asks for without claiming the laptop's memory controller
did the work.
"""

from __future__ import annotations

import json


def _encode_nibble(nibble: int) -> int:
    data_1 = (nibble >> 0) & 1
    data_2 = (nibble >> 1) & 1
    data_3 = (nibble >> 2) & 1
    data_4 = (nibble >> 3) & 1
    parity_1 = data_1 ^ data_2 ^ data_4
    parity_2 = data_1 ^ data_3 ^ data_4
    parity_4 = data_2 ^ data_3 ^ data_4
    return (
        parity_1
        | (parity_2 << 1)
        | (data_1 << 2)
        | (parity_4 << 3)
        | (data_2 << 4)
        | (data_3 << 5)
        | (data_4 << 6)
    )


def _decode_nibble(word: int) -> tuple[int, str]:
    def extract(value: int) -> tuple[int, int, int, int, int]:
        parity_1 = value & 1
        parity_2 = (value >> 1) & 1
        data_1 = (value >> 2) & 1
        parity_4 = (value >> 3) & 1
        data_2 = (value >> 4) & 1
        data_3 = (value >> 5) & 1
        data_4 = (value >> 6) & 1
        syndrome_1 = parity_1 ^ data_1 ^ data_2 ^ data_4
        syndrome_2 = parity_2 ^ data_1 ^ data_3 ^ data_4
        syndrome_4 = parity_4 ^ data_2 ^ data_3 ^ data_4
        syndrome = syndrome_1 | (syndrome_2 << 1) | (syndrome_4 << 2)
        nibble = data_1 | (data_2 << 1) | (data_3 << 2) | (data_4 << 3)
        return nibble, syndrome, data_1, data_2, data_3

    _nibble, syndrome, _d1, _d2, _d3 = extract(word)
    if syndrome == 0:
        nibble, _, _, _, _ = extract(word)
        return nibble, "ok"
    corrected = word ^ (1 << (syndrome - 1))
    nibble, remaining, _, _, _ = extract(corrected)
    if remaining != 0:
        return nibble, "uncorrectable"
    return nibble, "corrected"


def encode(data: bytes) -> bytes:
    coded = bytearray()
    for byte in data:
        coded.append(_encode_nibble((byte >> 4) & 0xF))
        coded.append(_encode_nibble(byte & 0xF))
    return bytes(coded)


def decode(coded: bytes) -> tuple[bytes, str]:
    if len(coded) % 2 != 0:
        raise ValueError("codeword length must be even")
    plain = bytearray()
    status = "ok"
    for index in range(0, len(coded), 2):
        high, high_status = _decode_nibble(coded[index])
        low, low_status = _decode_nibble(coded[index + 1])
        if "uncorrectable" in (high_status, low_status):
            status = "uncorrectable"
        elif status != "uncorrectable" and "corrected" in (high_status, low_status):
            status = "corrected"
        plain.append((high << 4) | low)
    return bytes(plain), status


def flip_bit(coded: bytes, bit_index: int) -> bytes:
    buffer = bytearray(coded)
    buffer[bit_index // 8] ^= 1 << (bit_index % 8)
    return bytes(buffer)


def demonstrate(message: bytes = b"tuition:5000") -> dict[str, object]:
    """Correct every single-bit flip of a protected record. Fail closed otherwise."""
    coded = encode(message)
    corrections = 0
    for byte_index, code_byte in enumerate(coded):
        for bit in range(7):
            damaged = bytearray(coded)
            damaged[byte_index] = code_byte ^ (1 << bit)
            recovered, status = decode(bytes(damaged))
            if recovered != message or status != "corrected":
                raise AssertionError(f"missed a one-bit error at byte {byte_index} bit {bit}")
            corrections += 1
    clean, clean_status = decode(coded)
    if clean != message or clean_status != "ok":
        raise AssertionError("undamaged codeword did not decode")
    return {
        "ok": True,
        "label": "software Hamming SEC model of ECC, not hardware ECC",
        "message": message.decode(),
        "coded_bytes": len(coded),
        "single_bit_corrections": corrections,
        "double_bit_note": "Two flipped bits in one nibble are outside the correction radius.",
    }


if __name__ == "__main__":
    print(json.dumps(demonstrate()))
