fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../protos";
  
    println!("cargo:rerun-if-changed={}", proto_dir);
  
    let proto_files = vec![
      "dataloader",
      "dataset",
      "distributed",
      "job"
    ];
    let protos: Vec<String> = proto_files
      .iter()
      .map(|f| format!("{}/{}.proto", proto_dir, f))
      .collect();
  
    tonic_build::configure()
      .out_dir("./src/proto")
      .compile(&protos, &[proto_dir.to_string()])
      .expect("Failed to compile grpc!");
    Ok(())
  }