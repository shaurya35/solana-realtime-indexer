# DESIGN

Design decisions for solana-realtime-indexer, and the reasoning behind each.

## Event identity

### Why `absolute_path` + `event_ordinal`?

<!-- 3-5 sentences. What is absolute_path, what does event_ordinal
     break a tie on, and why is this stable across replays? -->

### Why does `(signature, instruction_index)` collide?

<!-- You have real evidence: signature 3ud3k16… produced fills at
     absolute_path [3,5] and [6,5] in one transaction. Use it.
     What exactly would a naive indexer lose here? -->

## Storage

### Why are amounts BIGINT and never float?

<!-- What goes wrong with floats and money. Why decimals are a
     read-side concern. What u64 lamports means for the column type. -->

### Why are `events` and `trades` separate tables?

<!-- What each one represents. What becomes possible when decode
     logic changes and you have both. -->

### What does the checkpoint row mean, and when does it move?

<!-- Not built yet — answer from first principles.
     Why is SELECT MAX(slot) FROM events not equivalent? -->

## Divergence from Carbon's generated tables

<!-- Carbon generates one table per instruction type, keyed on
     (__signature, __instruction_index, __stack_height).
     One paragraph: what is that key good for, what is it not
     good for, and why is yours a different product rather
     than a better version of the same one? -->

## Backpressure

<!-- Left blank until Day 5. Your chosen overflow policy and the
     tradeoff you accepted goes here. -->