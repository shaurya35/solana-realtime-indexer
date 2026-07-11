use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> { 

    let pumpfun_program_id = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

    let content = 
        fs::read_to_string("fixtures/pumpfun-buy-via-flashx-01-parsed.json")?;

    let json: serde_json::Value = serde_json::from_str(&content)?;

    let inner_groups = json["result"]["meta"]["innerInstructions"]
        .as_array()
        .unwrap();

    for group in inner_groups {
        if group["index"].as_u64() == Some(3) {
            println!("Found group with index: {}", group["index"]);

            let instructions = group["instructions"]
                .as_array()
                .unwrap();

            for (position, instruction) in instructions.iter().enumerate() {
                if instruction["programId"].as_str() == Some(pumpfun_program_id) {
                    let encoded_data = instruction["data"]
                        .as_str()
                        .unwrap();

                    let decoded_data = bs58::decode(encoded_data)
                        .into_vec()?;

                        println!(
                            "Position: {}, Bytes: {}, First 8 bytes: {:?}",
                            position,
                            decoded_data.len(),
                            &decoded_data[..8]
                        );
                        
                }
            }
        }
    }

    Ok(())
}