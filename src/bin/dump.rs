use std::fs;
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let path = "fixtures/pumpfun-buy-via-flashx-01-parsed.json";

    let content = fs::read_to_string(path)?;
    let json: Value = serde_json::from_str(&content)?;

    let outer = json["result"]["transaction"]["message"]["instructions"]
        .as_array()
        .ok_or("No outer instruction array in fixture")?;

    println!("Outer instruction: {}", outer.len());

    let inner_groups = json["result"]["meta"]["innerInstructions"]
        .as_array()
        .ok_or("No Inner Instructions array")?;

    for (index, ix) in outer.iter().enumerate() {
        let program_id = ix["programId"].as_str().unwrap_or("?");
        let stack_height = ix["stackHeight"].as_u64().unwrap_or(1);
        let account_count = ix["accounts"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);

        println!("[{index}] program={program_id}  stack_height={stack_height}  accounts={account_count}  path={:?}", vec![index]);

        if let Some(group) = inner_groups.iter().find(|g| g["index"].as_u64() == Some(index as u64)) {
            if let Some(inners) = group["instructions"].as_array() {
                for (inner_idx, inner) in inners.iter().enumerate() {
                    let p = inner["programId"].as_str().unwrap_or("?");
                    let sh = inner["stackHeight"].as_u64().unwrap_or(0);
                    println!("      inner program={p}  stack_height={sh}  path={:?}", vec![index, inner_idx]);
                }
            }
        }
    }
    
    Ok(())
}