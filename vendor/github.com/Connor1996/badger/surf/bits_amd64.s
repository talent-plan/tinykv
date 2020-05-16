TEXT ·select64(SB),$24-24
    MOVQ    x+0(FP), AX
    MOVQ    k+8(FP), CX
    CMPB    ·hasBMI2(SB), $0
    JEQ     fallback
    DECQ    CX
    MOVQ    $1, BX
    SHLQ    CX, BX
    PDEPQ   AX, BX, BX
    TZCNTQ  BX, BX
    MOVQ    BX, ret+16(FP)
    RET
fallback:
    MOVQ    AX, (SP)
    MOVQ    CX, 8(SP)
    CALL    ·select64Broadword(SB)
    MOVQ    16(SP), AX
    MOVQ    AX, ret+16(FP)
    RET
