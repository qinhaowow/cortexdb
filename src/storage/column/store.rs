use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub struct ColumnStore {
    columns: Arc<RwLock<HashMap<String, Arc<Column>>>>,
    row_count: Arc<RwLock<usize>>,
}

pub struct Column {
    name: String,
    column_type: ColumnType,
    data: Vec<u8>,
    offsets: Vec<usize>,
    null_mask: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Boolean,
    Integer,
    Float,
    Double,
    String,
    Vector { dimension: usize },
    Timestamp,
}

impl ColumnStore {
    pub fn new() -> Self {
        Self {
            columns: Arc::new(RwLock::new(HashMap::new())),
            row_count: Arc::new(RwLock::new(0)),
        }
    }

    pub fn create_column(&self, name: String, column_type: ColumnType) -> Result<(), ColumnStoreError> {
        let mut columns = self.columns.write().unwrap();
        if columns.contains_key(&name) {
            return Err(ColumnStoreError::ColumnAlreadyExists(name));
        }

        let column = Arc::new(Column {
            name: name.clone(),
            column_type,
            data: Vec::new(),
            offsets: Vec::new(),
            null_mask: Vec::new(),
        });

        columns.insert(name, column);
        Ok(())
    }

    pub fn add_row(&self, values: HashMap<String, Option<ColumnValue>>) -> Result<usize, ColumnStoreError> {
        let columns = self.columns.read().unwrap();
        let mut row_count = self.row_count.write().unwrap();
        let current_row = *row_count;

        for (column_name, value) in values {
            if let Some(column) = columns.get(&column_name) {
                self.write_value(column, current_row, value)?;
            }
        }

        *row_count += 1;
        Ok(current_row)
    }

    pub fn get_row(&self, row_id: usize) -> Result<HashMap<String, Option<ColumnValue>>, ColumnStoreError> {
        let columns = self.columns.read().unwrap();
        let mut result = HashMap::new();

        for (name, column) in columns.iter() {
            let value = self.read_value(column, row_id)?;
            result.insert(name.clone(), value);
        }

        Ok(result)
    }

    pub fn get_column(&self, name: &str) -> Result<Arc<Column>, ColumnStoreError> {
        let columns = self.columns.read().unwrap();
        columns.get(name)
            .ok_or_else(|| ColumnStoreError::ColumnNotFound(name.to_string()))
            .map(|c| c.clone())
    }

    pub fn read_value(&self, column: &Column, row_id: usize) -> Result<Option<ColumnValue>, ColumnStoreError> {
        if row_id >= column.null_mask.len() {
            return Err(ColumnStoreError::RowNotFound(row_id));
        }

        if column.null_mask[row_id] {
            return Ok(None);
        }

        let reader = ColumnReader::new(column);
        reader.read(row_id)
    }

    pub fn write_value(&self, column: &Column, row_id: usize, value: Option<ColumnValue>) -> Result<(), ColumnStoreError> {
        let mut writer = ColumnWriter::new(column);
        writer.write(row_id, value)
    }

    pub fn row_count(&self) -> usize {
        *self.row_count.read().unwrap()
    }

    pub fn column_count(&self) -> usize {
        self.columns.read().unwrap().len()
    }

    pub fn list_columns(&self) -> Vec<String> {
        self.columns.read().unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ColumnStoreError {
    #[error("Column already exists: {0}")]
    ColumnAlreadyExists(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Row not found: {0}")]
    RowNotFound(usize),

    #[error("Invalid value for column type: {0}")]
    InvalidValue(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Clone)]
pub enum ColumnValue {
    Boolean(bool),
    Integer(i64),
    Float(f32),
    Double(f64),
    String(String),
    Vector(Vec<f32>),
    Timestamp(i64),
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    pub fn null_mask(&self) -> &[bool] {
        &self.null_mask
    }

    pub fn is_null(&self, row_id: usize) -> bool {
        if row_id < self.null_mask.len() {
            self.null_mask[row_id]
        } else {
            true
        }
    }
}

pub struct ColumnReader<'a> {
    column: &'a Column,
}

impl<'a> ColumnReader<'a> {
    pub fn new(column: &'a Column) -> Self {
        Self { column }
    }

    pub fn read(&self, row_id: usize) -> Result<Option<ColumnValue>, ColumnStoreError> {
        if row_id >= self.column.null_mask.len() {
            return Err(ColumnStoreError::RowNotFound(row_id));
        }

        if self.column.null_mask[row_id] {
            return Ok(None);
        }

        match &self.column.column_type {
            ColumnType::Boolean => {
                let offset = row_id;
                if offset >= self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                Ok(Some(ColumnValue::Boolean(self.column.data[offset] != 0)))
            },
            ColumnType::Integer => {
                let offset = row_id * 8;
                if offset + 8 > self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                let value = i64::from_le_bytes(self.column.data[offset..offset+8].try_into().unwrap());
                Ok(Some(ColumnValue::Integer(value)))
            },
            ColumnType::Float => {
                let offset = row_id * 4;
                if offset + 4 > self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                let value = f32::from_le_bytes(self.column.data[offset..offset+4].try_into().unwrap());
                Ok(Some(ColumnValue::Float(value)))
            },
            ColumnType::Double => {
                let offset = row_id * 8;
                if offset + 8 > self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                let value = f64::from_le_bytes(self.column.data[offset..offset+8].try_into().unwrap());
                Ok(Some(ColumnValue::Double(value)))
            },
            ColumnType::String => {
                let start = self.column.offsets[row_id];
                let end = if row_id + 1 < self.column.offsets.len() {
                    self.column.offsets[row_id + 1]
                } else {
                    self.column.data.len()
                };
                let value = String::from_utf8_lossy(&self.column.data[start..end]).to_string();
                Ok(Some(ColumnValue::String(value)))
            },
            ColumnType::Vector { dimension } => {
                let offset = row_id * dimension * 4;
                if offset + dimension * 4 > self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                let mut vector = Vec::with_capacity(*dimension);
                for i in 0..*dimension {
                    let value = f32::from_le_bytes(self.column.data[offset + i*4..offset + (i+1)*4].try_into().unwrap());
                    vector.push(value);
                }
                Ok(Some(ColumnValue::Vector(vector)))
            },
            ColumnType::Timestamp => {
                let offset = row_id * 8;
                if offset + 8 > self.column.data.len() {
                    return Err(ColumnStoreError::InternalError("Invalid data length".to_string()));
                }
                let value = i64::from_le_bytes(self.column.data[offset..offset+8].try_into().unwrap());
                Ok(Some(ColumnValue::Timestamp(value)))
            },
        }
    }
}

pub struct ColumnWriter<'a> {
    column: &'a Column,
}

impl<'a> ColumnWriter<'a> {
    pub fn new(column: &'a Column) -> Self {
        Self { column }
    }

    pub fn write(&mut self, row_id: usize, value: Option<ColumnValue>) -> Result<(), ColumnStoreError> {
        // Ensure null mask is large enough
        while self.column.null_mask.len() <= row_id {
            self.column.null_mask.push(true);
        }

        match value {
            None => {
                self.column.null_mask[row_id] = true;
                Ok(())
            },
            Some(val) => {
                self.column.null_mask[row_id] = false;
                
                match (&self.column.column_type, val) {
                    (ColumnType::Boolean, ColumnValue::Boolean(b)) => {
                        while self.column.data.len() <= row_id {
                            self.column.data.push(0);
                        }
                        self.column.data[row_id] = if b { 1 } else { 0 };
                        Ok(())
                    },
                    (ColumnType::Integer, ColumnValue::Integer(i)) => {
                        let offset = row_id * 8;
                        while self.column.data.len() < offset + 8 {
                            self.column.data.push(0);
                        }
                        self.column.data[offset..offset+8].copy_from_slice(&i.to_le_bytes());
                        Ok(())
                    },
                    (ColumnType::Float, ColumnValue::Float(f)) => {
                        let offset = row_id * 4;
                        while self.column.data.len() < offset + 4 {
                            self.column.data.push(0);
                        }
                        self.column.data[offset..offset+4].copy_from_slice(&f.to_le_bytes());
                        Ok(())
                    },
                    (ColumnType::Double, ColumnValue::Double(d)) => {
                        let offset = row_id * 8;
                        while self.column.data.len() < offset + 8 {
                            self.column.data.push(0);
                        }
                        self.column.data[offset..offset+8].copy_from_slice(&d.to_le_bytes());
                        Ok(())
                    },
                    (ColumnType::String, ColumnValue::String(s)) => {
                        while self.column.offsets.len() <= row_id {
                            self.column.offsets.push(self.column.data.len());
                        }
                        self.column.data.extend(s.as_bytes());
                        Ok(())
                    },
                    (ColumnType::Vector { dimension }, ColumnValue::Vector(v)) => {
                        if v.len() != *dimension {
                            return Err(ColumnStoreError::InvalidValue(format!("Expected vector dimension {} but got {}", dimension, v.len())))
                        }
                        let offset = row_id * dimension * 4;
                        while self.column.data.len() < offset + dimension * 4 {
                            self.column.data.push(0);
                        }
                        for i in 0..*dimension {
                            self.column.data[offset + i*4..offset + (i+1)*4].copy_from_slice(&v[i].to_le_bytes());
                        }
                        Ok(())
                    },
                    (ColumnType::Timestamp, ColumnValue::Timestamp(t)) => {
                        let offset = row_id * 8;
                        while self.column.data.len() < offset + 8 {
                            self.column.data.push(0);
                        }
                        self.column.data[offset..offset+8].copy_from_slice(&t.to_le_bytes());
                        Ok(())
                    },
                    (_, _) => {
                        Err(ColumnStoreError::InvalidValue("Type mismatch".to_string()))
                    },
                }
            },
        }
    }
}
