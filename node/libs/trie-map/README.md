Сделаю так:

1. Дам **конкретный список Rust-репозиториев** по трём семействам, о которых ты говоришь:
   – JMT / Libra-style SMT (Diem/Aptos, Penumbra, Sovereign, lsmtree)
   – “Cosmos-style” compacted SMT (Nervos, Namada, monotree, etc.)
   – Ethereum MPT / zkTrie (Substrate/Parity, eth-trie, zkTrie/Scroll)

2. Потом сравню **перфоманс на уровне конструкции**: число хэшей, I/O-паттерны, удобство для LSM/KV, ZK и т.д. — только то, что либо прямо следует из статей/доков, либо из очевидной асимптотики.

---

## 1. JMT / Libra-style Sparse Merkle Trees (LSM-friendly)

### Основные Rust-репозитории

**Базовая спецификация JMT**

* **Diem / Aptos**

  * crate `diem_jellyfish_merkle` внутри исходного Diem (Rust) – JMT как 256-битный sparse Merkle с оптимизацией: поддерево с 0 или 1 листом сворачивается в один узел, что резко сокращает число хэшей на “пустых” уровнях. ([diem.github.io][1])
  * crate `aptos-jellyfish-merkle` – реализация JMT в Aptos. ([crates.io][2])

* **Penumbra**

  * GitHub: `penumbra-zone/jmt` – “async-friendly” fork Diem JMT, выделенный в отдельную библиотеку. ([GitHub][3])
  * crate `jmt` на crates.io – тот же код, оформленный как crate. ([crates.io][4])

* **Sovereign Labs**

  * GitHub: `Sovereign-Labs/jellyfish-merkle-generic` – JMT, параметризованный по хэш-функции и размеру дайджеста, с флагами `metrics` и `rayon` (параллелизация вставок). Архив, перенесён в монорепу, но как Rust-реализация существует. ([GitHub][5])

* **Пост-квантовые форки Penumbra JMT**

  * crate `jmt-blake3` – JMT с BLAKE3. ([crates.io][6])
  * crate `jmt-pq` – форк Penumbra JMT c SHA-512, ориентирован на post-quantum устойчивость. ([lib.rs][7])

**Libra-style SMT в отдельной библиотеке**

* **`al8n/lsmtree`**
  GitHub: `al8n/lsmtree` – “Sparse Merkle tree for a key-value map”, прямо говорится, что реализованы оптимизации из Libra/JMT whitepaper, чтобы уменьшить число хэшей на операцию до `O(k)`, где `k` – число непустых элементов. ([GitHub][8])

---

## 2. “Cosmos-style” compacted SMT (общий SMT стек, используется в космос-экосистеме)

Тут Cosmos-SDK сам по себе на Go, но множество Cosmos-совместимых L1/L2 (Namada, Penumbra, др.) используют Rust-SMT, часто форки одного и того же кода.

### Базовые compacted SMT

* **Nervos Network**

  * GitHub: `nervosnetwork/sparse-merkle-tree` – “An optimized sparse merkle tree”, с явной таблицей асимптотик:
    размер дерева ≈ `2n + log n`, операции update / proof / verify – `O(log n)`; есть multi-leaf proofs и `no_std`. ([GitHub][9])

* **Namada / Anoma**

  * GitHub: `anoma/sparse-merkle-tree` (ранее `namada-net/sparse-merkle-tree`) – форк compacted SMT с такими же асимптотиками и поддержкой multi-leaf proofs, `no_std`. ([GitHub][10])

* **Aiken / Cardano tooling**

  * GitHub: `aiken-lang/sparse-merkle-tree` – off-chain Rust-реализация SMT, форканная из Nervos SMT и адаптированная к on-chain коду. ([GitHub][11])

### Другие Rust-SMT

* **monotree**

  * GitHub: `thyeem/monotree` – “Rust implementation of an optimized Sparse Merkle Tree”, бинарный radix-tree (ветвление по одному биту). Поддерживает inclusion / non-inclusion proof, несколько backends и hashers. ([GitHub][12])

* **light-sparse-merkle-tree**

  * crate `light-sparse-merkle-tree` – “sparse indexed (and concurrent) Merkle tree in Rust”; фокус на конкурентности и индексации. ([crates.io][13])

* **Merkle-Sum SMT**

  * GitHub: `keep-starknet-strange/mssmt-rs` – Merkle-Sum Sparse Merkle Tree (MS-SMT), т.е. SMT + суммирование значений в узлах (для балансов/лимитов). ([GitHub][14])

* **Учебные / простые**

  * GitHub: `antontroskie/sparse-merkle-tree` – базовая Rust-реализация SMT, больше как учебный проект. ([GitHub][15])

---

## 3. Ethereum-style Merkle-Patricia Trie (MPT) и zkTrie в Rust

### MPT (Ethereum-совместимые tries)

* **Substrate / Parity**

  * GitHub: `paritytech/trie` – generic Base-16 Modified Merkle Patricia Tree (MPT), с двумя основными crate’ами: `trie-db` (persistent trie с backend-базой) и `trie-root` (функция для вычисления root по key/value в памяти). ([GitHub][16])

* **eth-trie.rs**

  * GitHub: `carver/eth-trie.rs` – Rust-реализация Ethereum MPT (fork CITA-trie, вдохновлённый go-ethereum `trie`). Использует Keccak-256 и RLP-подобные структуры. ([GitHub][17])

* **ComposableFi Patricia-trie**

  * GitHub: `ComposableFi/patricia-merkle-trie` – верификация Ethereum-style MPT-доказательств (Keccak-256-based). ([GitHub][18])

* **CodeChain / Foundry**

  * GitHub: `CodeChain-io/rust-merkle-trie` – мерклезированный trie для состояния CodeChain/Foundry, совместимый но не идентичный МРТ Ethereum. ([GitHub][19])

### zkTrie / Sparse binary MPT (L2-style)

* **Scroll zkTrie**

  * GitHub: `scroll-tech/zktrie` – бинарный Poseidon-trie для Scroll; repo содержит Go и Rust реализации. ([GitHub][20])
  * GitHub: `scroll-tech/rust-zktrie` – “reimplementation of zktrie” на Rust. ([GitHub][21])
  * GitHub: `automata-network/scroll-zktrie` – Rust wrapper zktrie как crate. ([GitHub][22])
  * Документация Scroll прямо говорит, что zkTrie – “sparse binary Merkle tree with Poseidon hash”, оптимизированный для ZK-доказательств. ([gateway-docs.unruggable.com][23])

* **Эволюция Scroll: zkTrie → MPT**

  * В 2025 Scroll объявил, что **депрекирует zkTrie и переходит на MPT** как state commitment из-за лучшей производительности секвенсера и совместимости с dApp, которые полагаются на L2-state proofs. ([docs.scroll.io][24])

---

## 4. Сравнение перфоманса: JMT vs compacted SMT vs MPT/zkTrie (Rust-стек)

Тут важные моменты:

* почти **нет общих, стандартизованных бенчмарков “lib A vs lib B”**;
* зато есть:

  * формальная асимптотика в README/whitepaper’ах,
  * заявления самих проектов (Aptos, Penumbra, Scroll, Nervos и др.),
  * очевидная разница в конструкции (глубина, тип узлов, hash-функция, layout в KV-store).

Ниже – сравнительный обзор с чётким разделением: **что явно написано в источниках**, а что – прямое следствие конструкции.

### 4.1. Сколько хэшей и какой I/O за операцию

#### JMT / Libra-style (Diem, Aptos, Penumbra, Sovereign, lsmtree)

Факты:

* Логически JMT – **256-уровневое sparse дерево**, но поддерево с ≤1 листом **сворачивается в один узел** (leaf или placeholder). ([diem.github.io][1])
* Libra whitepaper и JMT-paper описывают оптимизацию, приводящую число хэш-операций на update к `O(k)`, где `k` – число непустых узлов по пути, а не фиксированные 256. ([developers.diem.com][25])
* `lsmtree` прямо повторяет эту формулировку: “reduce the number of hash operations required per tree operation to O(k) where k is the number of non-empty elements in the tree”. ([GitHub][8])

Следствия:

* Для **реально sparse состояний** (большинство ключей пустые) JMT/lsmtree делают **существенно меньше хэшей, чем наивный SMT глубины 256**, где ты бы всегда считал hash на всех уровнях.
* Поскольку JMT оптимизирован под LSM-storage (RocksDB и др.), layout ключей и node-key схемы построены так, чтобы **минимизировать compaction и write-amplification**. ([developers.diem.com][25])
* Aptos и внешние обзоры утверждают, что JMT даёт:

  * более быстрые запросы,
  * до ~15x экономии диска,
  * более высокий throughput по сравнению с более наивными деревьями. ([Aptos Network][26])

Вывод по перформансу (качественно):

* **Если state хранится в LSM (RocksDB/LevelDB и т.п.) и версия ключей монотонно растёт**, JMT / `lsmtree` находятся очень близко к “практическому optimum” по сочетанию CPU+I/O:
  меньше хэшей, меньше записей, меньше compaction.

#### Compacted SMT (Nervos, Namada, monotree, light-SMT)

Факты:

* Nervos / Namada SMT явно дают асимптотику: размер `2n + log n`, update / proof / verify – `O(log n)`; используют трюк с `merge` (если один из детей – ноль, просто возвращаем другой, без хэширования). ([GitHub][9])
* Эти библиотеки **не завязаны на версионные ключи**, но:

  * хэшируют только непустые ветки,
  * хранят дерево в `map[(height, parent)] -> (left, right)` (sparse layout). ([GitHub][10])

Следствия:

* По числу хэшей на update compacted SMT Nervos/Namada **очень близки к JMT**:
  тоже пропускают длинные цепочки нулей, хэшируя только “живую” часть пути.
* Разница с JMT больше в **формате узлов и key-layout для KV-store**, чем в асимптотике хэширования.

Отдельно:

* `monotree` реализует бинарный radix-trie (1 bit per level), с упором на простоту и поддержку разных backends/hasher’ов. ([GitHub][12])
* `light-sparse-merkle-tree` декларирует “concurrent” SMT – оптимизацией будет параллельное построение/обновление path’ов, что критично для высоких TPS. ([crates.io][27])

Качественный вывод:

* **По чистому CPU-cost per op** compacted SMT (Nervos/Namada/monotree/light-SMT) находятся в том же классе, что и JMT/lsmtree:
  `O(log n)` высота, но значительно меньше константы благодаря пропуску нулей.
* В отличие от JMT, они **менее заточены под конкретный LSM-layout**, но взамен проще как “общие” SMT-библиотеки.

#### Ethereum MPT / zkTrie (Substrate, eth-trie, zkTrie)

Факты:

* Ethereum MPT – **Base-16 Patricia Merkle Trie**: на каждом узле 16 детей (по nibble-ветвлению), используются сложные узлы типа branch/extension/leaf и сериализация (RLP). ([Medium][28])
* Rust-реализации:

  * `paritytech/trie` – generic Base-16 MPT (под Substrate). ([GitHub][16])
  * `carver/eth-trie.rs`, `ComposableFi/patricia-merkle-trie`, `CodeChain-io/rust-merkle-trie` – MPT для Ethereum-подобных клиентов. ([GitHub][17])
* zkTrie (Scroll):

  * “sparse binary Merkle tree with Poseidon hash” для zkEVM, Rust-реализация в `zktrie`/`rust-zktrie`. ([GitHub][20])
  * Scroll в 2025 официально говорит: **переходит с zkTrie на MPT** “to unlock better sequencer performance and better compatibility”. ([docs.scroll.io][24])

Следствия:

* В MPT:

  * глубина дерева по nibble’ам ≈ `2 * len(key)` байт,
  * но каждый шаг требует **больше работы**: RLP-encoding, Keccak-256, более сложные типы узлов.
* Поэтому, при одинаковом наборе ключей и криптографическом хэше, **константный фактор по CPU** у MPT обычно **хуже, чем у бинарного SMT/JMT**, где узлы намного проще.
* zkTrie заменяет Keccak/RLP на Poseidon и более простую структуру, чтобы сделать **доказательства в SNARK-циркуитах дешевле**, но при этом Poseidon тяжелее на обычном CPU, а trie остаётся глубокой sparse-структурой.

Факт из Scroll:

* Сам Scroll утверждает, что для **перформанса секвенсера** MPT оказался лучше zkTrie, при том что zkTrie изначально был оптимизирован под ZK. ([docs.scroll.io][24])

Качественный вывод:

* Для **он-чейн клиента без ZK**, бинарные SMT/JMT почти всегда будут **дешевле по CPU** на update/lookup, чем MPT/zkTrie при той же hash-функции.
* Для **ZK-применений** zkTrie / MS-SMT (`mssmt-rs`) выигрывают по “дешевизне доказательства” за счёт Poseidon/структуры дерева, но проигрывают по raw CPU/latency обычным Keccak-/Blake-библиотекам.

---

### 4.2. I/O и поведение поверх LSM / KV-хранилищ

**JMT / lsmtree / Penumbra:**

* JMT paper + Aptos docs подчёркивают: дерево **специально оптимизировано под LSM-KV (RocksDB)**, минимизируя compaction и write-amplification через продуманную схему ключей для узлов. ([developers.diem.com][25])
* Penumbra использует `jmt` с асинхронным доступом к state (“asynchronous reads, synchronous writes in CoW snapshots”). ([penumbra.zone][29])

**Compacted SMT (Nervos/Namada/monotree):**

* Фокус – **агностичность к storage backend’у** и `no_std`, часто в embedded/zk-контексте, а не глубокая интеграция с конкретным LSM. ([GitHub][9])

**MPT / zkTrie:**

* Ethereum MPT исторически сложился поверх LevelDB / RocksDB в go-ethereum, но без специализированного layout уровня JMT.
* zkTrie в Scroll оборачивает Go Ethereum в Rust + Poseidon hash, и, по отчётам аудитов Trail of Bits, требует кастомную инфраструктуру и локальный Scroll node для генерации proofs. ([Medium][30])

Вывод:

* Если ты строишь **новый L1/L2 на Rust + RocksDB/LSM**, JMT/`lsmtree`/Penumbra-`jmt` дают **наиболее “заточенный” под KV-store** вариант с минимумом I/O.
* Compact SMT – лучше для **генерика / ZK-окружения / embedded**.
* MPT/zkTrie – **нужны только для Ethereum-совместимости или ZK-эквивалентности Ethereum**, иначе перформанс/сложность не окупаются.

---

### 4.3. Конкретные “перформанс-лейблы” из текстов (без выдуманных цифр)

Ниже – только то, что **прямо написано** в источниках:

* **JMT (Diem/Aptos)**: “space- and computation-efficient sparse Merkle tree optimized for LSM-tree based key-value storage”, с меньшими proof’ами и меньшими вычислениями при верификации по сравнению с альтернативами в продакшене. ([developers.diem.com][25])
* **Aptos про JMT**: JMT “leverages monotonically increasing version-based key schema” для оптимизации записи в LSM, достигая “sweet spot between CPU, I/O and storage footprint”; внешний обзор упоминает до 15x экономии диска и рост throughput. ([Aptos Network][26])
* **lsmtree**: прямо декларирует `O(k)` хэш-операций на операцию (k – число непустых элементов) и что перформанс почти полностью зависит от используемого хэшера. ([GitHub][8])
* **Nervos/Namada SMT**: даётся таблица асимптотик (размер 2n+log n, update/get/proof/verify – log n) и описание оптимизации merge со специальным случаем нулей. ([GitHub][9])
* **Scroll zkTrie → MPT**: официально заявлено, что отказ от zkTrie в пользу MPT “unlocks better sequencer performance and better compatibility for dapps relying on L2 state proofs”. ([docs.scroll.io][24])

---

## 5. Практическое резюме по выбору

Если грубо сгруппировать по use-case:

* **L1/L2 с версионным состоянием + RocksDB/LSM + Rust**
  → смотри `jmt` (Penumbra), `aptos-jellyfish-merkle`, `lsmtree`.
  Они реализуют один и тот же JMT-класс алгоритмов, который доказанно хорошо ведёт себя на LSM.

* **Cosmos-подобный стек / модульное состояние / no_std / ZK-сценарии**
  → `nervosnetwork/sparse-merkle-tree`, `anoma/sparse-merkle-tree`, `monotree`, `light-sparse-merkle-tree`, `mssmt-rs`.
  Это compacted SMT без жёсткой привязки к LSM, но с сопоставимой асимптотикой.

* **EVM-совместимость или “Ethereum-эквивалентное” состояние**
  → `paritytech/trie`, `carver/eth-trie.rs`, `ComposableFi/patricia-merkle-trie`, `CodeChain-io/rust-merkle-trie` (MPT),
  → либо `zktrie`/`rust-zktrie`, если нужно ZK-эквивалентное состояние для zkEVM.

Если хочешь, в следующем шаге могу:

* вытащить конкретные параметры (тип хэша, layout ключей, версионность, наличие batched-updates/async API)
* и собрать **табличку “что выбрать для Acki Nacki”** с учётом твоей VM/LSM-архитектуры и требований к ZK.

[1]: https://diem.github.io/diem/diem_jellyfish_merkle/index.html "diem_jellyfish_merkle - Rust"
[2]: https://crates.io/crates/aptos-jellyfish-merkle?utm_source=chatgpt.com "aptos-jellyfish-merkle - crates.io: Rust Package Registry"
[3]: https://github.com/penumbra-zone/jmt "GitHub - penumbra-zone/jmt: An async-friendly sparse merkle tree implementation based on Diem's Jellyfish Merkle Tree"
[4]: https://crates.io/crates/jmt?utm_source=chatgpt.com "jmt - Penumbra's Jellyfish Merkle Tree"
[5]: https://github.com/Sovereign-Labs/jellyfish-merkle-generic "GitHub - Sovereign-Labs/jellyfish-merkle-generic: An implementation of a jellyfish merkle tree, generic over hash function and digest size"
[6]: https://crates.io/crates/jmt-blake3?utm_source=chatgpt.com "jmt-blake3 - Penumbra's Jellyfish Merkle Tree"
[7]: https://lib.rs/crates/jmt-pq?utm_source=chatgpt.com "jmt-pq - Post-Quantum Jellyfish Merkle Tree"
[8]: https://github.com/al8n/lsmtree "GitHub - al8n/lsmtree: Sparse Merkle tree for a key-value map."
[9]: https://github.com/nervosnetwork/sparse-merkle-tree "GitHub - nervosnetwork/sparse-merkle-tree"
[10]: https://github.com/anoma/sparse-merkle-tree "GitHub - namada-net/sparse-merkle-tree: An optimized sparse merkle tree."
[11]: https://github.com/aiken-lang/sparse-merkle-tree?utm_source=chatgpt.com "aiken-lang/sparse-merkle-tree"
[12]: https://github.com/thyeem/monotree "GitHub - thyeem/monotree: An optimized Sparse Merkle Tree in Rust"
[13]: https://crates.io/crates/light-sparse-merkle-tree/0.3.0/dependencies?utm_source=chatgpt.com "light-sparse-merkle-tree - crates.io: Rust Package Registry"
[14]: https://github.com/keep-starknet-strange/mssmt-rs?utm_source=chatgpt.com "Merkle-Sum Sparse Merkle Tree (MS-SMT) in Rust"
[15]: https://github.com/antontroskie/sparse-merkle-tree?utm_source=chatgpt.com "A Sparse Merkle Tree (SMT) implementation in Rust"
[16]: https://github.com/paritytech/trie "GitHub - paritytech/trie: Base-16 Modified Patricia Merkle Tree (aka Trie)"
[17]: https://github.com/carver/eth-trie.rs "GitHub - carver/eth-trie.rs: Rust implementation of the Modified Patricia Tree (aka Trie)."
[18]: https://github.com/ComposableFi/patricia-merkle-trie?utm_source=chatgpt.com "Ethereum style patricia merkle trie implementation"
[19]: https://github.com/CodeChain-io/rust-merkle-trie?utm_source=chatgpt.com "CodeChain-io/rust-merkle-trie"
[20]: https://github.com/scroll-tech/zktrie?utm_source=chatgpt.com "scroll-tech/zktrie: Binary storage trie"
[21]: https://github.com/scroll-tech/rust-zktrie?utm_source=chatgpt.com "scroll-tech/rust-zktrie"
[22]: https://github.com/automata-network/scroll-zktrie?utm_source=chatgpt.com "automata-network/scroll-zktrie"
[23]: https://gateway-docs.unruggable.com/chains/scroll?utm_source=chatgpt.com "Scroll - Gateway Documentation"
[24]: https://docs.scroll.io/en/technology/overview/scroll-upgrades/euclid-upgrade/?utm_source=chatgpt.com "Euclid Upgrade | Scroll Documentation"
[25]: https://developers.diem.com/papers/jellyfish-merkle-tree/2021-01-14.pdf "Jellyfish Merkle Tree"
[26]: https://aptosnetwork.com/currents/why-aptos-8-innovations-powering-aptos-network?utm_source=chatgpt.com "Why Aptos: 8 Innovations Powering the Aptos Network"
[27]: https://crates.io/crates/light-sparse-merkle-tree/0.3.0?utm_source=chatgpt.com "light-sparse-merkle-tree - crates.io: Rust Package Registry"
[28]: https://medium.com/ethereum-stories/merkle-patricia-trie-in-ethereum-a-silhouette-c8d04155b490?utm_source=chatgpt.com "Merkle Patricia Trie in Ethereum: A Silhouette"
[29]: https://www.penumbra.zone/blog/interchain-privacy-is-here?utm_source=chatgpt.com "Interchain Privacy is Here"
[30]: https://medium.com/iosg-ventures/exploring-developer-experience-on-zkrus-an-in-depth-analysis-785e1de3a7da?utm_source=chatgpt.com "Exploring Developer Experience on ZKRUs: An In-Depth ..."
