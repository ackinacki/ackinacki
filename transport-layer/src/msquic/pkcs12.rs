use getrandom::getrandom;
use hmac::Hmac;
use hmac::Mac;
use lazy_static::lazy_static;
use sha1::Digest;
use sha1::Sha1;
use yasna::models::ObjectIdentifier;
use yasna::ASN1Error;
use yasna::ASN1ErrorKind;
use yasna::DERWriter;
use yasna::Tag;

type HmacSha1 = Hmac<Sha1>;

fn as_oid(s: &'static [u64]) -> ObjectIdentifier {
    ObjectIdentifier::from_slice(s)
}

lazy_static! {
    static ref OID_DATA_CONTENT_TYPE: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 7, 1]);
    static ref OID_ENCRYPTED_DATA_CONTENT_TYPE: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 7, 6]);
    static ref OID_FRIENDLY_NAME: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 9, 20]);
    static ref OID_LOCAL_KEY_ID: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 9, 21]);
    static ref OID_CERT_TYPE_X509_CERTIFICATE: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 9, 22, 1]);
    static ref OID_CERT_TYPE_SDSI_CERTIFICATE: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 9, 22, 2]);
    static ref OID_PBE_WITH_SHA_AND3_KEY_TRIPLE_DESCBC: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 12, 1, 3]);
    static ref OID_SHA1: ObjectIdentifier = as_oid(&[1, 3, 14, 3, 2, 26]);
    static ref OID_PBE_WITH_SHA1_AND40_BIT_RC2_CBC: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 12, 1, 6]);
    static ref OID_KEY_BAG: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 1]);
    static ref OID_PKCS8_SHROUDED_KEY_BAG: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 2]);
    static ref OID_CERT_BAG: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 3]);
    static ref OID_CRL_BAG: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 4]);
    static ref OID_SECRET_BAG: ObjectIdentifier = as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 5]);
    static ref OID_SAFE_CONTENTS_BAG: ObjectIdentifier =
        as_oid(&[1, 2, 840, 113_549, 1, 12, 10, 1, 6]);
}

const ITERATIONS: u64 = 2048;

fn sha1(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    hasher.finalize().to_vec()
}

#[derive(Debug, Clone)]
pub struct EncryptedContentInfo {
    pub content_encryption_algorithm: AlgorithmIdentifier,
    pub encrypted_content: Vec<u8>,
}

impl EncryptedContentInfo {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            w.next().write_oid(&OID_DATA_CONTENT_TYPE);
            self.content_encryption_algorithm.write(w.next());
            w.next()
                .write_tagged_implicit(Tag::context(0), |w| w.write_bytes(&self.encrypted_content));
        })
    }

    pub fn from_safe_bags(safe_bags: &[SafeBag], password: &[u8]) -> Option<EncryptedContentInfo> {
        let data = yasna::construct_der(|w| {
            w.write_sequence_of(|w| {
                for sb in safe_bags {
                    sb.write(w.next());
                }
            })
        });
        let salt = rand()?.to_vec();
        let encrypted_content =
            pbe_with_sha1_and40_bit_rc2_cbc_encrypt(&data, password, &salt, ITERATIONS)?;
        let content_encryption_algorithm =
            AlgorithmIdentifier::PbeWithSHAAnd40BitRC2CBC(Pkcs12PbeParams {
                salt,
                iterations: ITERATIONS,
            });
        Some(EncryptedContentInfo { content_encryption_algorithm, encrypted_content })
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub encrypted_content_info: EncryptedContentInfo,
}

impl EncryptedData {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            w.next().write_u8(0);
            self.encrypted_content_info.write(w.next());
        })
    }

    pub fn from_safe_bags(safe_bags: &[SafeBag], password: &[u8]) -> Option<Self> {
        let encrypted_content_info = EncryptedContentInfo::from_safe_bags(safe_bags, password)?;
        Some(EncryptedData { encrypted_content_info })
    }
}

#[derive(Debug, Clone)]
pub enum ContentInfo {
    Data(Vec<u8>),
    EncryptedData(EncryptedData),
}

impl ContentInfo {
    pub fn write(&self, w: DERWriter) {
        match self {
            ContentInfo::Data(data) => w.write_sequence(|w| {
                w.next().write_oid(&OID_DATA_CONTENT_TYPE);
                w.next().write_tagged(Tag::context(0), |w| w.write_bytes(data))
            }),
            ContentInfo::EncryptedData(encrypted_data) => w.write_sequence(|w| {
                w.next().write_oid(&OID_ENCRYPTED_DATA_CONTENT_TYPE);
                w.next().write_tagged(Tag::context(0), |w| encrypted_data.write(w))
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pkcs12PbeParams {
    pub salt: Vec<u8>,
    pub iterations: u64,
}

impl Pkcs12PbeParams {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            w.next().write_bytes(&self.salt);
            w.next().write_u64(self.iterations);
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OtherAlgorithmIdentifier {
    pub algorithm_type: ObjectIdentifier,
    pub params: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlgorithmIdentifier {
    Sha1,
    PbeWithSHAAnd40BitRC2CBC(Pkcs12PbeParams),
    PbeWithSHAAnd3KeyTripleDESCBC(Pkcs12PbeParams),
}

impl AlgorithmIdentifier {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| match self {
            AlgorithmIdentifier::Sha1 => {
                w.next().write_oid(&OID_SHA1);
                w.next().write_null();
            }
            AlgorithmIdentifier::PbeWithSHAAnd40BitRC2CBC(p) => {
                w.next().write_oid(&OID_PBE_WITH_SHA1_AND40_BIT_RC2_CBC);
                p.write(w.next());
            }
            AlgorithmIdentifier::PbeWithSHAAnd3KeyTripleDESCBC(p) => {
                w.next().write_oid(&OID_PBE_WITH_SHA_AND3_KEY_TRIPLE_DESCBC);
                p.write(w.next());
            }
        })
    }
}

#[derive(Debug)]
pub struct DigestInfo {
    pub digest_algorithm: AlgorithmIdentifier,
    pub digest: Vec<u8>,
}

impl DigestInfo {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            self.digest_algorithm.write(w.next());
            w.next().write_bytes(&self.digest);
        })
    }
}

#[derive(Debug)]
pub struct MacData {
    pub mac: DigestInfo,
    pub salt: Vec<u8>,
    pub iterations: u32,
}

impl MacData {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            self.mac.write(w.next());
            w.next().write_bytes(&self.salt);
            w.next().write_u32(self.iterations);
        })
    }

    pub fn new(data: &[u8], password: &[u8]) -> MacData {
        let salt = rand().unwrap();
        let key = pbepkcs12sha1(password, &salt, ITERATIONS, 3, 20);
        let mut mac = HmacSha1::new_from_slice(&key).unwrap();
        mac.update(data);
        let digest = mac.finalize().into_bytes().to_vec();
        MacData {
            mac: DigestInfo { digest_algorithm: AlgorithmIdentifier::Sha1, digest },
            salt: salt.to_vec(),
            iterations: ITERATIONS as u32,
        }
    }
}

fn rand() -> Option<[u8; 8]> {
    let mut buf = [0u8; 8];
    if getrandom(&mut buf).is_ok() {
        Some(buf)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct Pfx {
    pub version: u8,
    pub auth_safe: ContentInfo,
    pub mac_data: Option<MacData>,
}

impl Pfx {
    pub fn new(
        cert_der: &[u8],
        key_der: &[u8],
        ca_der: Option<&[u8]>,
        password: &str,
        name: &str,
    ) -> Option<Pfx> {
        let mut cas = vec![];
        if let Some(ca) = ca_der {
            cas.push(ca);
        }
        Self::new_with_cas(cert_der, key_der, &cas, password, name)
    }

    pub fn new_with_cas(
        cert_der: &[u8],
        key_der: &[u8],
        ca_der_list: &[&[u8]],
        password: &str,
        name: &str,
    ) -> Option<Pfx> {
        let friendly_name = PKCS12Attribute::FriendlyName(name.to_owned());
        let local_key_id = PKCS12Attribute::LocalKeyId(sha1(cert_der));
        let (key_bag, password) = if password.is_empty() {
            let bag = SafeBag {
                bag: SafeBagKind::Key(key_der.to_vec()),
                attributes: vec![friendly_name.clone(), local_key_id.clone()],
            };
            (bag, vec![])
        } else {
            let password = bmp_string(password);
            let salt = rand()?.to_vec();
            let encrypted_data = pbe_with_sha_and3_key_triple_des_cbc_encrypt(
                key_der, &password, &salt, ITERATIONS,
            )?;
            let bag = SafeBag {
                bag: SafeBagKind::Pkcs8ShroudedKey(EncryptedPrivateKeyInfo {
                    encryption_algorithm: AlgorithmIdentifier::PbeWithSHAAnd3KeyTripleDESCBC(
                        Pkcs12PbeParams { salt, iterations: ITERATIONS },
                    ),
                    encrypted_data,
                }),
                attributes: vec![friendly_name.clone(), local_key_id.clone()],
            };
            (bag, password)
        };

        let cert_bag_inner = SafeBagKind::Cert(CertBag::X509(cert_der.to_owned()));
        let cert_bag =
            SafeBag { bag: cert_bag_inner, attributes: vec![friendly_name, local_key_id] };
        let mut cert_bags = vec![cert_bag];
        for ca in ca_der_list {
            cert_bags.push(SafeBag {
                bag: SafeBagKind::Cert(CertBag::X509((*ca).to_owned())),
                attributes: vec![],
            });
        }
        let contents = yasna::construct_der(|w| {
            w.write_sequence_of(|w| {
                if password.is_empty() {
                    ContentInfo::Data(yasna::construct_der(|w| {
                        w.write_sequence_of(|w| {
                            for cert_bag in cert_bags {
                                cert_bag.write(w.next());
                            }
                        })
                    }))
                    .write(w.next());
                } else {
                    ContentInfo::EncryptedData(
                        EncryptedData::from_safe_bags(&cert_bags, &password)
                            .ok_or_else(|| ASN1Error::new(ASN1ErrorKind::Invalid))
                            .unwrap(),
                    )
                    .write(w.next());
                }
                ContentInfo::Data(yasna::construct_der(|w| {
                    w.write_sequence_of(|w| {
                        key_bag.write(w.next());
                    })
                }))
                .write(w.next());
            });
        });
        let mac_data = Some(MacData::new(&contents, &password));
        Some(Pfx { version: 3, auth_safe: ContentInfo::Data(contents), mac_data })
    }

    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            w.next().write_u8(self.version);
            self.auth_safe.write(w.next());
            if let Some(mac_data) = &self.mac_data {
                mac_data.write(w.next())
            }
        })
    }

    pub fn to_der(&self) -> Vec<u8> {
        yasna::construct_der(|w| self.write(w))
    }
}

#[inline(always)]
fn pbepkcs12sha1core(d: &[u8], i: &[u8], a: &mut Vec<u8>, iterations: u64) -> Vec<u8> {
    let mut ai: Vec<u8> = d.iter().chain(i.iter()).cloned().collect();
    for _ in 0..iterations {
        ai = sha1(&ai);
    }
    a.append(&mut ai.clone());
    ai
}

#[allow(clippy::many_single_char_names)]
fn pbepkcs12sha1(pass: &[u8], salt: &[u8], iterations: u64, id: u8, size: u64) -> Vec<u8> {
    const U: u64 = 160 / 8;
    const V: u64 = 512 / 8;
    let r: u64 = iterations;
    let d = [id; V as usize];
    fn get_len(s: usize) -> usize {
        let s = s as u64;
        (V * s.div_ceil(V)) as usize
    }
    let s = salt.iter().cycle().take(get_len(salt.len()));
    let p = pass.iter().cycle().take(get_len(pass.len()));
    let mut i: Vec<u8> = s.chain(p).cloned().collect();
    let c = size.div_ceil(U);
    let mut a: Vec<u8> = vec![];
    for _ in 1..c {
        let ai = pbepkcs12sha1core(&d, &i, &mut a, r);

        let b: Vec<u8> = ai.iter().cycle().take(V as usize).cloned().collect();

        let b_iter = b.iter().rev().cycle().take(i.len());
        let i_b_iter = i.iter_mut().rev().zip(b_iter);
        let mut inc = 1u8;
        for (i3, (ii, bi)) in i_b_iter.enumerate() {
            if ((i3 as u64) % V) == 0 {
                inc = 1;
            }
            let (ii2, inc2) = ii.overflowing_add(*bi);
            let (ii3, inc3) = ii2.overflowing_add(inc);
            inc = (inc2 || inc3) as u8;
            *ii = ii3;
        }
    }

    pbepkcs12sha1core(&d, &i, &mut a, r);

    a.iter().take(size as usize).cloned().collect()
}

fn pbe_with_sha1_and40_bit_rc2_cbc_encrypt(
    data: &[u8],
    password: &[u8],
    salt: &[u8],
    iterations: u64,
) -> Option<Vec<u8>> {
    use cbc::cipher::block_padding::Pkcs7;
    use cbc::cipher::BlockEncryptMut;
    use cbc::cipher::KeyIvInit;
    use cbc::Encryptor;
    use rc2::Rc2;
    type Rc2Cbc = Encryptor<Rc2>;

    let dk = pbepkcs12sha1(password, salt, iterations, 1, 5);
    let iv = pbepkcs12sha1(password, salt, iterations, 2, 8);

    let rc2 = Rc2Cbc::new_from_slices(&dk, &iv).ok()?;
    Some(rc2.encrypt_padded_vec_mut::<Pkcs7>(data))
}

fn pbe_with_sha_and3_key_triple_des_cbc_encrypt(
    data: &[u8],
    password: &[u8],
    salt: &[u8],
    iterations: u64,
) -> Option<Vec<u8>> {
    use cbc::cipher::block_padding::Pkcs7;
    use cbc::cipher::BlockEncryptMut;
    use cbc::cipher::KeyIvInit;
    use cbc::Encryptor;
    use des::TdesEde3;
    type TDesCbc = Encryptor<TdesEde3>;

    let dk = pbepkcs12sha1(password, salt, iterations, 1, 24);
    let iv = pbepkcs12sha1(password, salt, iterations, 2, 8);

    let tdes = TDesCbc::new_from_slices(&dk, &iv).ok()?;
    Some(tdes.encrypt_padded_vec_mut::<Pkcs7>(data))
}

fn bmp_string(s: &str) -> Vec<u8> {
    let utf16: Vec<u16> = s.encode_utf16().collect();

    let mut bytes = Vec::with_capacity(utf16.len() * 2 + 2);
    for c in utf16 {
        bytes.push((c / 256) as u8);
        bytes.push((c % 256) as u8);
    }
    bytes.push(0x00);
    bytes.push(0x00);
    bytes
}

#[derive(Debug, Clone)]
pub enum CertBag {
    X509(Vec<u8>),
}

impl CertBag {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| match self {
            CertBag::X509(x509) => {
                w.next().write_oid(&OID_CERT_TYPE_X509_CERTIFICATE);
                w.next().write_tagged(Tag::context(0), |w| w.write_bytes(x509));
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncryptedPrivateKeyInfo {
    pub encryption_algorithm: AlgorithmIdentifier,
    pub encrypted_data: Vec<u8>,
}

impl EncryptedPrivateKeyInfo {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            self.encryption_algorithm.write(w.next());
            w.next().write_bytes(&self.encrypted_data);
        })
    }
}

#[derive(Debug, Clone)]
pub enum SafeBagKind {
    Key(Vec<u8>),
    Pkcs8ShroudedKey(EncryptedPrivateKeyInfo),
    Cert(CertBag),
    // CRLBag(),
    // SecretBag(),
    // SafeContents(Vec<SafeBag>),
}

impl SafeBagKind {
    pub fn write(&self, w: DERWriter) {
        match self {
            SafeBagKind::Key(kb) => w.write_der(kb),
            SafeBagKind::Pkcs8ShroudedKey(epk) => epk.write(w),
            SafeBagKind::Cert(cb) => cb.write(w),
        }
    }

    pub fn oid(&self) -> ObjectIdentifier {
        match self {
            SafeBagKind::Key(_) => OID_KEY_BAG.clone(),
            SafeBagKind::Pkcs8ShroudedKey(_) => OID_PKCS8_SHROUDED_KEY_BAG.clone(),
            SafeBagKind::Cert(_) => OID_CERT_BAG.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PKCS12Attribute {
    FriendlyName(String),
    LocalKeyId(Vec<u8>),
}

impl PKCS12Attribute {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| match self {
            PKCS12Attribute::FriendlyName(name) => {
                w.next().write_oid(&OID_FRIENDLY_NAME);
                w.next().write_set_of(|w| {
                    w.next().write_bmp_string(name);
                })
            }
            PKCS12Attribute::LocalKeyId(id) => {
                w.next().write_oid(&OID_LOCAL_KEY_ID);
                w.next().write_set_of(|w| w.next().write_bytes(id))
            }
        })
    }
}
#[derive(Debug, Clone)]
pub struct SafeBag {
    pub bag: SafeBagKind,
    pub attributes: Vec<PKCS12Attribute>,
}

impl SafeBag {
    pub fn write(&self, w: DERWriter) {
        w.write_sequence(|w| {
            w.next().write_oid(&self.bag.oid());
            w.next().write_tagged(Tag::context(0), |w| self.bag.write(w));
            if !self.attributes.is_empty() {
                w.next().write_set_of(|w| {
                    for attr in &self.attributes {
                        attr.write(w.next());
                    }
                })
            }
        })
    }
}
