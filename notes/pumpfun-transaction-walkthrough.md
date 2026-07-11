# Pumpfun Buy via FLASHX

## Identity

- Signature: 3pXo1Y7332tzwq9DSuMTHr5dtuDfKgsUCHSrwReY59yt6WrkcDZnky6yxuugymX3qzgaPsSo6vB4kTN9ab6VaVJq
- Slot: 432193607
- Successful: yes
- Protocol operation: Pumpfun Buy
- Router: FLASHX

## Execution Path

- Outer Instruction 3 calls FLASHX at stack height 1.
- FLASHX calls Pumpfun as inner instruction 00 at stack height 2.
- Pumpfun calls fee, Token-2022 and System programs at stack height 3.
- Pumpfun emits a self-CPI event as inner instruction 06 at stack height 3.
- Control returns to FLASHX for inner instruction 07 at stack height 2.

## Token movement

- Mint: 2KjpDfEZeA3LHcq1ycHi5qYf9Lc5D1iJtLhSHKUypump
- Source token account: 5n5vdxc6hD4DKeBt6DDQcv3kAZA7n5sAmVqgH3ySMU1b
- Destination token account: 2WiWC7CqmtdjXSazCZGqeeynZTDVbwD7HiVTYcDY626k
- Raw amount: 3940708338
- Decimals: 6
- Display amount: 3940.708338

## SOL movement

- Transaction fee: 15000 lamports
- Payer total decrease: 125000 lamports
- Pumpfun-nested transfers observed: 99000 lamports
- FLASHX-level transfer observed: 1000 lamports
- Some transfer semantics remain unclassified until event decoding.

## Core lesson

Looking only at outer instructions would miss the Pumpfun trade because Pumpfun was invoked through FLASHX. The Pumpfun Buy instruction establishes intent, while the CPI event contains the executed trade result.