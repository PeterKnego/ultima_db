import Lake
open Lake DSL

package «btree_proofs»

/- The Aeneas Lean library, pinned to the release the kernel was translated
   with (see formal/README.md). Populate `formal/.toolchain` first via
   `formal/scripts/fetch-toolchain.sh` — it downloads prebuilt binaries and a
   prebuilt library, so no OCaml or Lean-library source build is needed. -/
require aeneas from "../.toolchain/backends/lean"

lean_lib «BtreeKernel»
lean_lib «BtreeInvariant»
lean_lib «ListLemmas»
lean_lib «AlignedLemmas»
lean_lib «ChildrenSpecs»
lean_lib «FindPosSpec»
lean_lib «EntrySpecs»
lean_lib «TransportLemmas»

@[default_target]
lean_lib «BtreeProofs»

@[default_target]
lean_lib «BtreeInsertInv»

@[default_target]
lean_lib «BtreeInsertGet»

@[default_target]
lean_lib «BtreeInsertFrame»
