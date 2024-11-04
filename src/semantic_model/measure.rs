use super::{SemanticModel, SemanticModelStoreError};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum MeasureType {
    Simple,
    Ratio,
    Cumulative,
    Derived,
}

impl Default for MeasureType {
    fn default() -> Self {
        MeasureType::Simple
    }
}

#[derive(Debug, Serialize, Clone)]
pub enum Measure {
    Simple(SimpleMeasure),
    Ratio(RatioMeasure),
    Cumulative(CumulativeMeasure),
    Derived(DerivedMeasure),
}

impl Measure {
    pub fn name(&self) -> &str {
        match self {
            Measure::Simple(simple) => &simple.name,
            Measure::Ratio(ratio) => &ratio.name,
            Measure::Cumulative(cumulative) => &cumulative.name,
            Measure::Derived(derived) => &derived.name,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SimpleMeasure {
    pub name: String,
    pub description: String,
    pub data_type: String,
    pub aggregation: String,
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RatioPart {
    pub name: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct RatioMeasure {
    pub name: String,
    pub description: String,
    pub data_type: String,
    pub numerator: RatioPart,
    pub denominator: RatioPart,
}

#[derive(Debug, Serialize, Clone)]
pub struct CumulativeMeasure {
    pub name: String,
    pub description: String,
    pub data_type: String,
    pub aggregation: Option<String>,
    pub sql: String,
    pub partition_by: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DerivedMeasurePart {
    pub name: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct DerivedMeasure {
    pub name: String,
    pub description: String,
    pub sql: String,
    pub measures: Vec<DerivedMeasurePart>,
}

impl<'de> Deserialize<'de> for Measure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct MeasureHelper {
            name: String,
            description: String,
            data_type: String,
            sql: Option<String>,
            aggregation: Option<String>,
            numerator: Option<RatioPart>,
            denominator: Option<RatioPart>,
            partition_by: Option<Vec<String>>,
            #[serde(default)]
            measure_type: MeasureType,
            measures: Option<Vec<DerivedMeasurePart>>,
        }

        let helper = MeasureHelper::deserialize(deserializer)?;

        match helper.measure_type {
            MeasureType::Simple => Ok(Measure::Simple(SimpleMeasure {
                name: helper.name,
                description: helper.description,
                data_type: helper.data_type,
                aggregation: helper.aggregation.unwrap_or_default(),
                sql: helper.sql.unwrap_or_default(),
            })),
            MeasureType::Ratio => Ok(Measure::Ratio(RatioMeasure {
                name: helper.name,
                description: helper.description,
                data_type: helper.data_type,
                numerator: helper.numerator.unwrap_or(RatioPart {
                    name: String::new(),
                }),
                denominator: helper.denominator.unwrap_or(RatioPart {
                    name: String::new(),
                }),
            })),
            MeasureType::Cumulative => Ok(Measure::Cumulative(CumulativeMeasure {
                name: helper.name,
                description: helper.description,
                data_type: helper.data_type,
                sql: helper.sql.unwrap_or_default(),
                aggregation: helper.aggregation,
                partition_by: helper.partition_by.unwrap_or_default(),
            })),
            MeasureType::Derived => Ok(Measure::Derived(DerivedMeasure {
                name: helper.name,
                description: helper.description,
                sql: helper.sql.unwrap_or_default(),
                measures: helper.measures.unwrap_or_default(),
            })),
        }
    }
}

pub trait Renderable {
    fn render(
        &self,
        semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError>;
}

impl Renderable for SimpleMeasure {
    fn render(
        &self,
        _semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError> {
        Ok(render_sql(
            &self.sql,
            &self.name,
            Some(&self.aggregation),
            alias,
        ))
    }
}

impl Renderable for RatioMeasure {
    fn render(
        &self,
        semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError> {
        let numerator = semantic_model.get_measure(&self.numerator.name)?;
        let denominator = semantic_model.get_measure(&self.denominator.name)?;

        let numerator_sql = numerator.render(semantic_model, false)?;
        let denominator_sql = denominator.render(semantic_model, false)?;

        let sql = format!("({}) / NULLIFZERO({})", numerator_sql, denominator_sql);
        Ok(render_sql(&sql, &self.name, None, alias))
    }
}

impl Renderable for CumulativeMeasure {
    fn render(
        &self,
        _semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError> {
        let partition_by_sql = self.partition_by.join(", ");
        let sql = render_sql(&self.sql, &self.name, self.aggregation.as_deref(), false);
        let sql = format!("{} OVER (PARTITION BY {})", sql, partition_by_sql);
        Ok(render_sql(&sql, &self.name, None, alias))
    }
}

impl Renderable for DerivedMeasure {
    fn render(
        &self,
        semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError> {
        // For each measure, find and replace the alias with the actual field
        let mut sql = self.sql.clone();
        for measure in &self.measures {
            let measure_sql = semantic_model
                .get_measure(&measure.name)?
                .render(semantic_model, false)?;
            sql = sql.replace(&measure.name, &measure_sql);
        }
        Ok(render_sql(&sql, &self.name, None, alias))
    }
}

impl Renderable for Measure {
    fn render(
        &self,
        semantic_model: &SemanticModel,
        alias: bool,
    ) -> Result<String, SemanticModelStoreError> {
        match self {
            Measure::Simple(simple) => simple.render(semantic_model, alias),
            Measure::Ratio(ratio) => ratio.render(semantic_model, alias),
            Measure::Cumulative(cumulative) => cumulative.render(semantic_model, alias),
            Measure::Derived(derived) => derived.render(semantic_model, alias),
        }
    }
}

fn render_sql(sql: &str, name: &str, aggregation: Option<&str>, alias: bool) -> String {
    let sql = match aggregation {
        Some("sum") => format!("SUM({})", sql),
        Some("avg") => format!("AVG({})", sql),
        Some("median") => format!("MEDIAN({})", sql),
        Some("count") => format!("COUNT({})", sql),
        Some("count_distinct") => format!("COUNT(DISTINCT {})", sql),
        Some("min") => format!("MIN({})", sql),
        Some("max") => format!("MAX({})", sql),
        _ => sql.to_string(),
    };

    if !alias {
        return sql;
    }

    format!("{} AS {}", sql, name)
}
