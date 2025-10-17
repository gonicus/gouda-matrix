mod messages;

use std::io::{stdin, stdout, Read, Write};
use messages::{GreetRequest, GreetResponse};
use prost::Message;

fn read_size() -> u64 {
    let mut buf = [0; 8];
    stdin().read_exact(&mut buf).expect("error reading size buffer");
    u64::from_le_bytes(buf)
}

fn read_request(len: u64) -> GreetRequest {
    let mut buf = vec![0; len as usize];
    stdin().read_exact(&mut buf).expect("error reading buffer of size {len}");
    GreetRequest::decode(&mut std::io::Cursor::new(&buf as &[u8])).expect("error decoding greet request")
}

fn main() {
    loop {    
        // Input
        let size = read_size();
        let request = read_request(size);
        
        // Output
        let response = GreetResponse {
            result: request.x as i64 + request.y as i64,
            greeting: format!("Hallo, {} {}", request.prename, request.surname),
        };
    
        let serialized = response.encode_to_vec();
    
        stdout().write_all(&serialized.len().to_le_bytes()).expect("error writing size");
        stdout().write_all(&serialized).expect("error writing string");
        stdout().flush().expect("error flushing stdout");
    }
}
