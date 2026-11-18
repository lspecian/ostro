//! ostro-partition: per-partition state store (skeleton over redb).

use std::path::Path;

/// Open (or create) the partition's redb database at `path`.
pub fn open(path: impl AsRef<Path>) -> Result<redb::Database, redb::DatabaseError> {
    redb::Database::create(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{Durability, ReadableDatabase, TableDefinition};

    const TABLE: TableDefinition<&str, u64> = TableDefinition::new("kv");

    #[test]
    fn durability_none_then_immediate() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let db = open(dir.path().join("p.redb"))?;

        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::None)?;
        {
            let mut t = tx.open_table(TABLE)?;
            t.insert("a", 1)?;
        }
        tx.commit()?;

        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut t = tx.open_table(TABLE)?;
            t.insert("b", 2)?;
        }
        tx.commit()?;

        let rx = db.begin_read()?;
        let t = rx.open_table(TABLE)?;
        assert_eq!(t.get("a")?.map(|v| v.value()), Some(1));
        assert_eq!(t.get("b")?.map(|v| v.value()), Some(2));
        Ok(())
    }
}
