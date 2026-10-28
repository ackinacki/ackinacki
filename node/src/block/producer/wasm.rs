use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::anyhow;
use tvm_vm::executor::Engine;

const WASM_BINARY_ROOT_PATH: &str = "./config/wasm";
const NODE_MANDATORY_HASHES: [&str; 2] = [
    "b8891b913656ae35d9ffff371f0f03e4f1f869d0e17556a8c273750313884b0a",
    "343268736f6dbb5a075a477fb1146b3c25c114d341b41c142e6609a7d1a90a2c",
];

const WASM_COMPONENT_OVERRIDES_ENGINE_VERSION: semver::Version = semver::Version::new(1, 0, 8);
#[cfg(not(test))]
const WASM_COMPONENT_OVERRIDES: &[(&str, &str)] = &[(
    "2d577ca2e693700282d6d778dce8cfcedbada644497e411ec6aed889f5a3d5f4",
    "0d69c25d36f1d8814c644ce96c61dbfbd8ae13e9a116d7d9f379fbaf0648b0eb",
)];

pub fn check_node_mandatory_wasm_available() {
    for hash in NODE_MANDATORY_HASHES {
        let filename = format!("{WASM_BINARY_ROOT_PATH}/{hash}");
        log::debug!("Getting file {filename:?}");
        // Panics if no wasm file
        assert!(std::fs::exists(filename).unwrap());
    }
}

#[derive(Clone)]
pub struct WasmNodeCache {
    // cached resources used for wasm execution
    pub(crate) wasm_engine: wasmtime::Engine,
    pub(crate) wasm_binary_root_path: String,
    pub(crate) wasm_hash_whitelist: HashSet<[u8; 32]>,
    pub(crate) wasm_component_cache: HashMap<[u8; 32], wasmtime::component::Component>,
    wasm_component_cache_with_overrides: HashMap<[u8; 32], wasmtime::component::Component>,
}

impl WasmNodeCache {
    pub fn new() -> anyhow::Result<Self> {
        let wasm_binary_root_path = WASM_BINARY_ROOT_PATH.to_owned();
        let wasm_hash_whitelist = match Self::get_wasm_hash_whitelist(){
            Ok(r) => r,
            Err(e) => Err(anyhow!("Couldn't create Node Wasm Hash Whitelist. This is a misconfiguration or a bug. Caused by: {e:?}"))?,
        };
        let (wasm_engine, wasm_component_cache) = match Self::init_wasm_engine_and_precompile_components(
            wasm_hash_whitelist.clone(),
            wasm_binary_root_path.clone(),
        ){
            Ok(r) => r,
            Err(e) => Err(anyhow!("Couldn't init Node Wasm Engine or Components. This is a misconfiguration or a bug. Caused by: {e:?}"))?,
        };
        #[cfg(not(test))]
        let wasm_component_cache_with_overrides = match Self::apply_wasm_component_overrides(
            &wasm_engine,
            &wasm_binary_root_path,
            wasm_component_cache.clone(),
        ) {
            Ok(r) => r,
            Err(e) => Err(anyhow!("Couldn't apply Node Wasm component overrides. This is a misconfiguration or a bug. Caused by: {e:?}"))?,
        };
        #[cfg(test)]
        let wasm_component_cache_with_overrides = HashMap::new();
        Ok(WasmNodeCache {
            wasm_engine,
            wasm_binary_root_path,
            wasm_hash_whitelist,
            wasm_component_cache,
            wasm_component_cache_with_overrides,
        })
    }

    pub fn for_engine_version(&self, engine_version: &semver::Version) -> Self {
        let mut cache = self.clone();
        if *engine_version >= WASM_COMPONENT_OVERRIDES_ENGINE_VERSION {
            tracing::trace!(target: "monit", "override wasm_component");
            cache.wasm_component_cache = self.wasm_component_cache_with_overrides.clone();
        }
        cache
    }

    pub fn get_wasm_hash_whitelist() -> anyhow::Result<HashSet<[u8; 32]>> {
        // Hashes that are allowed for execution. These include mandatory wasm binaries, test binaries and ALL other allowed binaries.
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
            "b8891b913656ae35d9ffff371f0f03e4f1f869d0e17556a8c273750313884b0a",
            "2d577ca2e693700282d6d778dce8cfcedbada644497e411ec6aed889f5a3d5f4",
            "343268736f6dbb5a075a477fb1146b3c25c114d341b41c142e6609a7d1a90a2c",
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
                    return Err(anyhow::format_err!("hash whitelist has an invlid hash: {e:?}"))
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

    #[cfg(not(test))]
    fn hash_from_hex(hash_str: &str) -> anyhow::Result<[u8; 32]> {
        if hash_str.len() != 64 {
            return Err(anyhow::format_err!("hash must be 64 hex chars, got {}", hash_str.len()));
        }
        let bytes: Vec<u8> = (0..hash_str.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hash_str[i..i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(|e| anyhow::format_err!("invalid hex in hash: {e:?}"))?;
        bytes.try_into().map_err(|e| anyhow::format_err!("hash is not 32 bytes: {e:?}"))
    }

    #[cfg(not(test))]
    fn apply_wasm_component_overrides(
        wasm_engine: &wasmtime::Engine,
        wasm_binary_root_path: &str,
        mut cache: HashMap<[u8; 32], wasmtime::component::Component>,
    ) -> anyhow::Result<HashMap<[u8; 32], wasmtime::component::Component>> {
        tracing::trace!(target: "monit", "apply_wasm_component_overrides");
        use sha2::Digest;
        for (served_as, replacement_sha256) in WASM_COMPONENT_OVERRIDES {
            let served_as = Self::hash_from_hex(served_as)?;
            let expected = Self::hash_from_hex(replacement_sha256)?;
            let path = format!("{wasm_binary_root_path}/{replacement_sha256}");
            let binary = std::fs::read(&path)
                .map_err(|e| anyhow::format_err!("override binary {path:?} not readable: {e:?}"))?;
            let actual: [u8; 32] = sha2::Sha256::digest(&binary).into();
            if actual != expected {
                return Err(anyhow::format_err!(
                    "override binary {path:?} sha256 {} does not match the pinned {}",
                    hex::encode(actual),
                    hex::encode(expected),
                ));
            }
            let component = wasmtime::component::Component::new(wasm_engine, binary.as_slice())
                .map_err(|e| {
                    anyhow::format_err!("override binary {path:?} failed to compile: {e:?}")
                })?;
            log::warn!(
                "WASM override active: hash {} now runs {path:?} (sha256 {})",
                hex::encode(served_as),
                hex::encode(expected),
            );
            cache.insert(served_as, component);
        }
        Ok(cache)
    }

    pub fn init_wasm_engine() -> anyhow::Result<wasmtime::Engine> {
        match tvm_vm::executor::Engine::extern_wasm_engine_init() {
            Ok(en) => Ok(en),
            Err(er) => Err(anyhow::format_err!("Could not initialise Wasm engine. Check your execution environment for compatibility. Original error: {er:?}")),
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
