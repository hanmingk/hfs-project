pub mod chunkserver;
pub mod masterserver;

pub mod cfg {
    pub const REPLICA_BASE_DIR: &str = "~/hfs/data/chunkpool";

    pub const REPLICA_FINALIZED_DIR: &str = "~/hfs/data/chunkpool/finalized";

    pub const REPLICA_TMP_DIR: &str = "~/hfs/data/chunkpool/tmp";

    pub const DIR_INDEX_NAME: &str = "subdir";

    pub const DEFAULT_REPLICA_SIZE: usize = 64 * 1024 * 1024; // 64mb

    pub const NUM_REPLICA: usize = 2;
}
