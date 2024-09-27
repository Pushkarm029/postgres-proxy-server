use std::str::FromStr;

#[derive(Debug, PartialEq, Eq)]
pub enum PostgresType {
    Serial,
    BigSerial,
    SmallInt,
    Integer,
    BigInt,
    Numeric,
    Real,
    DoublePrecision,
    Money,
    ByteA,
    Varchar,
    Char,
    Text,
    Cidr,
    Inet,
    MacAddr,
    MacAddr8,
    Bit,
    Uuid,
    Xml,
    Json,
    Jsonb,
    TsVector,
    TsQuery,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Interval,
    Point,
    Line,
    LSeg,
    Box,
    Path,
    Polygon,
    Circle,
    Geometry,
    Array,
    Composite,
    Range,
    Oid,
    PgLsn,
    Boolean,
    Name,
    SLTimestamp,
}

impl FromStr for PostgresType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "serial" => Ok(PostgresType::Serial),
            "bigserial" => Ok(PostgresType::BigSerial),
            "int2" => Ok(PostgresType::SmallInt),
            "int4" => Ok(PostgresType::Integer),
            "int8" => Ok(PostgresType::BigInt),
            "numeric" => Ok(PostgresType::Numeric),
            "float4" => Ok(PostgresType::Real),
            "float8" => Ok(PostgresType::DoublePrecision),
            "money" => Ok(PostgresType::Money),
            "bytea" => Ok(PostgresType::ByteA),
            "varchar" => Ok(PostgresType::Varchar),
            "bpchar" => Ok(PostgresType::Char),
            "text" => Ok(PostgresType::Text),
            "cidr" => Ok(PostgresType::Cidr),
            "inet" => Ok(PostgresType::Inet),
            "macaddr" => Ok(PostgresType::MacAddr),
            "macaddr8" => Ok(PostgresType::MacAddr8),
            "bit" => Ok(PostgresType::Bit),
            "uuid" => Ok(PostgresType::Uuid),
            "xml" => Ok(PostgresType::Xml),
            "json" => Ok(PostgresType::Json),
            "jsonb" => Ok(PostgresType::Jsonb),
            "tsvector" => Ok(PostgresType::TsVector),
            "tsquery" => Ok(PostgresType::TsQuery),
            "timestamp" => Ok(PostgresType::Timestamp),
            "timestamptz" => Ok(PostgresType::TimestampTz),
            "date" => Ok(PostgresType::Date),
            "time" => Ok(PostgresType::Time),
            "timetz" => Ok(PostgresType::TimeTz),
            "interval" => Ok(PostgresType::Interval),
            "point" => Ok(PostgresType::Point),
            "line" => Ok(PostgresType::Line),
            "lseg" => Ok(PostgresType::LSeg),
            "box" => Ok(PostgresType::Box),
            "path" => Ok(PostgresType::Path),
            "polygon" => Ok(PostgresType::Polygon),
            "circle" => Ok(PostgresType::Circle),
            "geometry" => Ok(PostgresType::Geometry),
            "array" => Ok(PostgresType::Array),
            "composite" => Ok(PostgresType::Composite),
            "range" => Ok(PostgresType::Range),
            "oid" => Ok(PostgresType::Oid),
            "pg_lsn" => Ok(PostgresType::PgLsn),
            "bool" => Ok(PostgresType::Boolean),
            "name" => Ok(PostgresType::Name),
            "sl_timestamp" => Ok(PostgresType::SLTimestamp),
            _ => Err(format!("Unknown PostgreSQL type: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            PostgresType::from_str("int4").unwrap(),
            PostgresType::Integer
        );
        assert_eq!(
            PostgresType::from_str("varchar").unwrap(),
            PostgresType::Varchar
        );
        assert_eq!(
            PostgresType::from_str("timestamp").unwrap(),
            PostgresType::Timestamp
        );
        assert_eq!(
            PostgresType::from_str("bool").unwrap(),
            PostgresType::Boolean
        );
    }

    #[test]
    fn test_from_str_unknown_type() {
        assert!(PostgresType::from_str("unknown_type").is_err());
    }
}
