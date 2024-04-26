pub use hfs_proto::master::OPPermissions;

#[derive(Debug, Clone, Copy)]
pub struct Permissions {
    pub(crate) group_id: u16,
    pub(crate) owner_id: u16,
    pub(crate) op_perms: [OPPermissions; 3], // owner, group, other
}

impl Default for Permissions {
    fn default() -> Self {
        Self {
            group_id: u16::MAX,
            owner_id: u16::MAX,
            op_perms: [
                OPPermissions::ReadWithWrite,
                OPPermissions::ReadWithWrite,
                OPPermissions::ReadOnly,
            ],
        }
    }
}

pub fn perms_from(byte: u8) -> Option<OPPermissions> {
    match byte {
        1 => Some(OPPermissions::ReadOnly),
        2 => Some(OPPermissions::WriteOnly),
        3 => Some(OPPermissions::ReadWithWrite),
        4 => Some(OPPermissions::NoPermissions),
        _ => None,
    }
}

pub fn perms_to(perms: &OPPermissions) -> u8 {
    match perms {
        OPPermissions::ReadOnly => 1,
        OPPermissions::WriteOnly => 2,
        OPPermissions::ReadWithWrite => 3,
        OPPermissions::NoPermissions => 4,
    }
}

impl Permissions {
    pub fn from_bytes(bytes: &[u8]) -> Option<Permissions> {
        if bytes.len() < 7 {
            return None;
        }

        let user_perms = perms_from(bytes[4])?;
        let group_perms = perms_from(bytes[5])?;
        let other_perms = perms_from(bytes[6])?;

        Some(Permissions {
            group_id: u16::from_be_bytes(bytes[0..2].try_into().unwrap()),
            owner_id: u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
            op_perms: [user_perms, group_perms, other_perms],
        })
    }

    pub fn to_bytes(&self) -> [u8; 7] {
        let mut bytes = [0u8; 7];

        bytes[0..2].copy_from_slice(&self.group_id.to_be_bytes());
        bytes[2..4].copy_from_slice(&self.owner_id.to_be_bytes());

        bytes[4] = perms_to(&self.op_perms[0]);
        bytes[5] = perms_to(&self.op_perms[1]);
        bytes[6] = perms_to(&self.op_perms[2]);

        bytes
    }

    pub fn is_unset(&self) -> bool {
        if self.group_id == u16::MAX && self.owner_id == u16::MAX {
            return true;
        }
        false
    }

    pub fn with_id(owner_id: u16, group_id: u16) -> Permissions {
        Permissions {
            group_id,
            owner_id,
            op_perms: [
                OPPermissions::ReadWithWrite,
                OPPermissions::ReadWithWrite,
                OPPermissions::ReadOnly,
            ],
        }
    }

    pub fn op_perms_mask(&self) -> u8 {
        let mut mask: u8 = 0;

        for permission in self.op_perms.iter() {
            mask <<= 1;
            mask |= match permission {
                OPPermissions::ReadOnly => 1,
                OPPermissions::WriteOnly => 2,
                OPPermissions::ReadWithWrite => 3,
                OPPermissions::NoPermissions => 0,
            };
        }
        mask
    }
}
