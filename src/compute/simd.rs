use std::arch::x86_64::*;

pub struct SimdOperations {
    avx2_available: bool,
    avx_available: bool,
    sse_available: bool,
}

impl SimdOperations {
    pub fn new() -> Result<Self, SimdError> {
        Ok(Self {
            avx2_available: is_x86_feature_detected!("avx2"),
            avx_available: is_x86_feature_detected!("avx"),
            sse_available: is_x86_feature_detected!("sse"),
        })
    }

    pub fn is_available(&self) -> bool {
        self.sse_available
    }

    pub fn execute_operation(
        &self,
        op: VectorOperation,
        a: &[f32],
        b: &[f32],
    ) -> Result<Vec<f32>, SimdError> {
        match op {
            VectorOperation::Add => self.add(a, b),
            VectorOperation::Subtract => self.subtract(a, b),
            VectorOperation::Multiply => self.multiply(a, b),
            VectorOperation::Divide => self.divide(a, b),
            VectorOperation::DotProduct => {
                let result = self.dot_product(a, b)?;
                Ok(vec![result])
            },
            VectorOperation::CosineSimilarity => {
                let result = self.cosine_similarity(a, b)?;
                Ok(vec![result])
            },
            VectorOperation::EuclideanDistance => {
                let result = self.euclidean_distance(a, b)?;
                Ok(vec![result])
            },
        }
    }

    pub fn add(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let mut result = Vec::with_capacity(a.len());
        result.resize(a.len(), 0.0);

        if self.avx2_available {
            unsafe { self.add_avx2(a, b, &mut result) }
        } else if self.avx_available {
            unsafe { self.add_avx(a, b, &mut result) }
        } else if self.sse_available {
            unsafe { self.add_sse(a, b, &mut result) }
        } else {
            self.add_scalar(a, b, &mut result)
        }

        Ok(result)
    }

    pub fn subtract(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let mut result = Vec::with_capacity(a.len());
        result.resize(a.len(), 0.0);

        if self.avx2_available {
            unsafe { self.subtract_avx2(a, b, &mut result) }
        } else if self.avx_available {
            unsafe { self.subtract_avx(a, b, &mut result) }
        } else if self.sse_available {
            unsafe { self.subtract_sse(a, b, &mut result) }
        } else {
            self.subtract_scalar(a, b, &mut result)
        }

        Ok(result)
    }

    pub fn multiply(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let mut result = Vec::with_capacity(a.len());
        result.resize(a.len(), 0.0);

        if self.avx2_available {
            unsafe { self.multiply_avx2(a, b, &mut result) }
        } else if self.avx_available {
            unsafe { self.multiply_avx(a, b, &mut result) }
        } else if self.sse_available {
            unsafe { self.multiply_sse(a, b, &mut result) }
        } else {
            self.multiply_scalar(a, b, &mut result)
        }

        Ok(result)
    }

    pub fn divide(&self, a: &[f32], b: &[f32]) -> Result<Vec<f32>, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let mut result = Vec::with_capacity(a.len());
        result.resize(a.len(), 0.0);

        if self.avx2_available {
            unsafe { self.divide_avx2(a, b, &mut result) }
        } else if self.avx_available {
            unsafe { self.divide_avx(a, b, &mut result) }
        } else if self.sse_available {
            unsafe { self.divide_sse(a, b, &mut result) }
        } else {
            self.divide_scalar(a, b, &mut result)
        }

        Ok(result)
    }

    pub fn dot_product(&self, a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        if self.avx2_available {
            Ok(unsafe { self.dot_product_avx2(a, b) })
        } else if self.avx_available {
            Ok(unsafe { self.dot_product_avx(a, b) })
        } else if self.sse_available {
            Ok(unsafe { self.dot_product_sse(a, b) })
        } else {
            Ok(self.dot_product_scalar(a, b))
        }
    }

    pub fn cosine_similarity(&self, a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let dot = self.dot_product(a, b)?;
        let norm_a = self.euclidean_norm(a)?;
        let norm_b = self.euclidean_norm(b)?;

        if norm_a == 0.0 || norm_b == 0.0 {
            return Ok(0.0);
        }

        Ok(dot / (norm_a * norm_b))
    }

    pub fn euclidean_distance(&self, a: &[f32], b: &[f32]) -> Result<f32, SimdError> {
        if a.len() != b.len() {
            return Err(SimdError::InvalidVectorLength);
        }

        let mut sum = 0.0;
        for (x, y) in a.iter().zip(b.iter()) {
            let diff = x - y;
            sum += diff * diff;
        }

        Ok(sum.sqrt())
    }

    pub fn euclidean_norm(&self, a: &[f32]) -> Result<f32, SimdError> {
        let mut sum = 0.0;
        for &x in a {
            sum += x * x;
        }
        Ok(sum.sqrt())
    }

    #[target_feature(enable = "avx2")]
    unsafe fn add_avx2(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_add_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] + b[j];
        }
    }

    #[target_feature(enable = "avx")]
    unsafe fn add_avx(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_add_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] + b[j];
        }
    }

    #[target_feature(enable = "sse")]
    unsafe fn add_sse(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm_add_ps(a_chunk, b_chunk);
            _mm_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] + b[j];
        }
    }

    fn add_scalar(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            result[i] = x + y;
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn subtract_avx2(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_sub_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] - b[j];
        }
    }

    #[target_feature(enable = "avx")]
    unsafe fn subtract_avx(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_sub_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] - b[j];
        }
    }

    #[target_feature(enable = "sse")]
    unsafe fn subtract_sse(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm_sub_ps(a_chunk, b_chunk);
            _mm_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] - b[j];
        }
    }

    fn subtract_scalar(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            result[i] = x - y;
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn multiply_avx2(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_mul_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] * b[j];
        }
    }

    #[target_feature(enable = "avx")]
    unsafe fn multiply_avx(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_mul_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] * b[j];
        }
    }

    #[target_feature(enable = "sse")]
    unsafe fn multiply_sse(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm_mul_ps(a_chunk, b_chunk);
            _mm_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] * b[j];
        }
    }

    fn multiply_scalar(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            result[i] = x * y;
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn divide_avx2(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_div_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] / b[j];
        }
    }

    #[target_feature(enable = "avx")]
    unsafe fn divide_avx(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm256_div_ps(a_chunk, b_chunk);
            _mm256_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] / b[j];
        }
    }

    #[target_feature(enable = "sse")]
    unsafe fn divide_sse(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let res_chunk = _mm_div_ps(a_chunk, b_chunk);
            _mm_storeu_ps(result.as_mut_ptr().add(i), res_chunk);
            i += chunk_size;
        }

        for j in i..len {
            result[j] = a[j] / b[j];
        }
    }

    fn divide_scalar(&self, a: &[f32], b: &[f32], result: &mut [f32]) {
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            result[i] = x / y;
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn dot_product_avx2(&self, a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let mul = _mm256_mul_ps(a_chunk, b_chunk);
            let h_sum = _mm256_hadd_ps(mul, _mm256_setzero_ps());
            let mut temp = [0.0; 8];
            _mm256_storeu_ps(temp.as_mut_ptr(), h_sum);
            sum += temp[0] + temp[2] + temp[4] + temp[6];
            i += chunk_size;
        }

        for j in i..len {
            sum += a[j] * b[j];
        }

        sum
    }

    #[target_feature(enable = "avx")]
    unsafe fn dot_product_avx(&self, a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i));
            let mul = _mm256_mul_ps(a_chunk, b_chunk);
            let h_sum = _mm256_hadd_ps(mul, _mm256_setzero_ps());
            let mut temp = [0.0; 8];
            _mm256_storeu_ps(temp.as_mut_ptr(), h_sum);
            sum += temp[0] + temp[2] + temp[4] + temp[6];
            i += chunk_size;
        }

        for j in i..len {
            sum += a[j] * b[j];
        }

        sum
    }

    #[target_feature(enable = "sse")]
    unsafe fn dot_product_sse(&self, a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let mul = _mm_mul_ps(a_chunk, b_chunk);
            let h_sum = _mm_hadd_ps(mul, _mm_setzero_ps());
            let v_sum = _mm_hadd_ps(h_sum, _mm_setzero_ps());
            sum += _mm_cvtss_f32(v_sum);
            i += chunk_size;
        }

        for j in i..len {
            sum += a[j] * b[j];
        }

        sum
    }

    fn dot_product_scalar(&self, a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SimdError {
    #[error("Invalid vector length")]
    InvalidVectorLength,

    #[error("SIMD not available")]
    SimdNotAvailable,

    #[error("Internal error: {0}")]
    InternalError(String),
}

use crate::compute::runtime::VectorOperation;
