// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_abi::TokenValue;
use tvm_block::Account;
use tvm_client::encoding::slice_from_cell;
use tvm_types::UInt256;

use crate::auth::TokenIssuer;

const OWNER_WALLET_SIGN_KEY: &str = "_signing_pubkey";
const BK_LICENSES_MAP: &str = "_licenses";
const BM_LICENSE_NUMBER: &str = "_license_num";

pub static BK_OWNER_WALLET_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json"
);
pub static BM_OWNER_WALLET_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json"
);

#[derive(Debug, Default)]
pub struct OwnerWalletData {
    pub pubkey: Option<String>,
    pub license_count: usize,
}

pub fn decode_owner_wallet(
    account: &Account,
    issuer: &TokenIssuer,
) -> anyhow::Result<OwnerWalletData> {
    let Some(data) = account.get_data() else {
        return Ok(OwnerWalletData::default());
    };

    let slice = slice_from_cell(data)
        .map_err(|e| anyhow::format_err!("Failed to decode BK/BM Owner Wallet data cell: {e}"))?;

    let (abi_str, is_bk) = match issuer {
        TokenIssuer::Bk(_) => (BK_OWNER_WALLET_ABI, true),
        TokenIssuer::Bm(_) => (BM_OWNER_WALLET_ABI, false),
    };
    let abi = tvm_client::abi::Abi::Json(abi_str.to_string()).abi()?;

    let decoded = abi
        .decode_storage_fields(slice, true)
        .map_err(|e| anyhow::format_err!("Failed to decode BK/BM Owner Wallet storage: {e}"))?;

    let mut license_count = 0;
    let mut pubkey = None;

    for token in decoded {
        let token_name = token.name.as_str();

        if is_bk && token_name == BK_LICENSES_MAP {
            if let TokenValue::Map(.., c) = token.value {
                license_count = c.len();
            }
        } else if !is_bk && token_name == BM_LICENSE_NUMBER {
            if let TokenValue::Optional(_, Some(_)) = token.value {
                license_count = 1;
            }
        } else if token_name == OWNER_WALLET_SIGN_KEY {
            if let TokenValue::Uint(value) = token.value {
                let pubkey_uint = UInt256::from_be_bytes(&value.number.to_bytes_be());
                pubkey = Some(pubkey_uint.to_hex_string());
            }
        }
    }

    Ok(OwnerWalletData { pubkey, license_count })
}

// #[inline]
// fn abi_uint_to_usize_lossy(value: &Uint) -> usize {
//     let bytes = value.number.to_bytes_le();
//     let limit = bytes.len().min(std::mem::size_of::<usize>());

//     bytes
//         .iter()
//         .take(limit)
//         .enumerate()
//         .fold(0usize, |acc, (i, &byte)| acc | ((byte as usize) << (i << 3)))
// }

#[cfg(test)]
mod tests {
    use sdk_wrapper::read_file;
    use tvm_block::Account;
    use tvm_block::Deserializable;

    use crate::auth::TokenIssuer;
    use crate::owner_wallet::decode_owner_wallet;

    #[test]
    fn test_bm_owner_wallet() -> anyhow::Result<()> {
        let boc = String::from_utf8(read_file("./fixtures/real_bm_issuer_c3c0474d61fdd004960d1a5a320bc88549f73238cfa0a8fc6a00e15a72bbda19.boc.b64")?)
            .map(|s| s.trim().to_owned())?;

        let real_issuer_pubkey =
            "184ee76138c4b0f3b24096482c2e9be18c26214c5501290afe1df16f9e40e905".to_string();
        let real_issuer_account = Account::construct_from_base64(&boc)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))?;
        let issuer = TokenIssuer::Bm(real_issuer_pubkey.clone());
        let bm_wallet_data = decode_owner_wallet(&real_issuer_account, &issuer)?;

        assert_eq!(bm_wallet_data.pubkey, Some(real_issuer_pubkey));
        assert_eq!(bm_wallet_data.license_count, 1);

        let boc = String::from_utf8(read_file("./fixtures/fake_bm_issuer_40f11d13cf70edb0b4ac883a915c03ba333eceb49263e47ef0ba0415e9b023c5.boc.b64")?)
            .map(|s| s.trim().to_owned())?;

        let fake_issuer_pubkey =
            "95c9241701b8023509d1d55aaa0fe44ca7348822b1accf99c89a642379121153".to_string();
        let fake_issuer_account = Account::construct_from_base64(&boc)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))?;
        let issuer = TokenIssuer::Bm(fake_issuer_pubkey.clone());
        let bm_wallet_data = decode_owner_wallet(&fake_issuer_account, &issuer)?;

        assert_eq!(bm_wallet_data.pubkey, Some(fake_issuer_pubkey));
        assert_eq!(bm_wallet_data.license_count, 0);

        Ok(())
    }

    #[test]
    fn test_bk_owner_wallet() -> anyhow::Result<()> {
        let boc = String::from_utf8(read_file("./fixtures/real_bk_issuer_dc67ae73b647b399bb8293ef1b5c9bc9577baf036ad7ec5c49505efbae74ac67.boc.b64")?)
            .map(|s| s.trim().to_owned())?;

        let real_issuer_pubkey =
            "0a61873357b136f5daa35f9cc852113330864ab9ed6485dc36398520612c86e3".to_string();
        let account = Account::construct_from_base64(&boc)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))?;
        let real_issuer_issuer = TokenIssuer::Bk(real_issuer_pubkey.clone());
        let bm_wallet_data = decode_owner_wallet(&account, &real_issuer_issuer)?;

        assert_eq!(bm_wallet_data.pubkey, Some(real_issuer_pubkey));
        assert_eq!(bm_wallet_data.license_count, 20);

        Ok(())
    }
}
