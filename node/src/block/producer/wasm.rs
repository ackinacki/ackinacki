use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::anyhow;
use tvm_vm::executor::Engine;

#[derive(Clone)]
pub struct WasmNodeCache {
    // cached resources used for wasm execution
    pub(crate) wasm_engine: wasmtime::Engine,
    pub(crate) wasm_binary_root_path: String,
    pub(crate) wasm_hash_whitelist: HashSet<[u8; 32]>,
    pub(crate) wasm_component_cache: HashMap<[u8; 32], wasmtime::component::Component>,
}

impl WasmNodeCache {
    pub fn new() -> anyhow::Result<Self> {
        let wasm_binary_root_path = "./config/wasm".to_owned();
        let wasm_hash_whitelist = match Self::get_wasm_hash_whitelist(){
            Ok(r) => r,
            Err(e) => Err(anyhow!("Couldn't create Node Wasm Hash Whitelist. This is a misconfiguration or a bug. Caused by: {:?}", e))?,
        };
        let (wasm_engine, wasm_component_cache) = match Self::init_wasm_engine_and_precompile_components(
            wasm_hash_whitelist.clone(),
            wasm_binary_root_path.clone(),
        ){
            Ok(r) => r,
            Err(e) => Err(anyhow!("Couldn't init Node Wasm Engine or Components. This is a misconfiguration or a bug. Caused by: {:?}", e))?,
        };
        Ok(WasmNodeCache {
            wasm_engine,
            wasm_binary_root_path,
            wasm_hash_whitelist,
            wasm_component_cache,
        })
    }

    pub fn get_wasm_hash_whitelist() -> anyhow::Result<HashSet<[u8; 32]>> {
        let hash_strs = [
            "c5b3fe1a4fa391e9660a13d55ca2200f9343d5b1d18473ebbee19d8219e3ddc1",
            "7b7f96a857a4ada292d7c6b1f47940dde33112a2c2bc15b577dff9790edaeef2",
            "e88c99c9a1cbbde5bf47839db7685953c3bf266945f3270abb731ed84d58d163",
            "e7adc782c05b67bcda5babaca1deabf80f30ca0e6cf668c89825286c3ce0e560",
            "afbe8c5a02df7d6fa5decd4d48ff0f74ecbd4dae38bb5144328354db6bd95967",
            "25dc3d80d7e4d8f27dfadc9c2faf9cf2d8dea0a9e08a692da2db7e34d74d66e1",
            "d4a067079c3ff4e0b0b6f579ef2d1b9a1d8fc21a0076162503ff46a6e8fca2e5",
            "f6b0cc30d023d266819b16dafa5a6a6ad25b97246bbbca80abac2df974939b87",
            "7670910579bb17bf986de6e318c6f5a8bf7e148b3fb8e0cbf03479fb9eb8c948",
        ];
        let mut whitelist = HashSet::<[u8; 32]>::new();
        for hash_str in hash_strs {
            let hash: Vec<u8> = (0..hash_str.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hash_str[i..i + 2], 16).unwrap())
                .collect::<Vec<u8>>();
            let hash: [u8; 32] = match hash.try_into() {
                Ok(h) => h,
                Err(e) => {
                    return Err(anyhow::format_err!("hash whitelist has an invlid hash: {:?}", e))
                }
            };
            whitelist.insert(hash);
        }
        // let whitelist = tvm_vm::executor::Engine::extern_load_wasm_hash_whitelist_from_path(
        //     "config/wasm.conf".to_string().to_owned(),
        // )
        // .unwrap();
        Ok(whitelist)
    }

    pub fn init_wasm_engine() -> anyhow::Result<wasmtime::Engine> {
        match tvm_vm::executor::Engine::extern_wasm_engine_init() {
            Ok(en) => Ok(en),
            Err(er) => Err(anyhow::format_err!("Could not initialise Wasm engine. Check your execution environment for compatibility. Original error: {:?}", er)),
        }
    }

    pub fn init_wasm_engine_and_precompile_components(
        wasm_hash_whitelist: HashSet<[u8; 32]>,
        wasm_binary_root_path: String,
    ) -> anyhow::Result<(wasmtime::Engine, HashMap<[u8; 32], wasmtime::component::Component>)> {
        let engine = Self::init_wasm_engine()?;
        Ok((
            engine.clone(),
            Engine::extern_precompile_all_wasm_from_hash_list(
                wasm_binary_root_path,
                engine,
                wasm_hash_whitelist,
            ),
        ))
    }
}
