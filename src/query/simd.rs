use std::arch::x86_64::*;

pub struct SimdOperations;

impl SimdOperations {
    #[target_feature(enable = "avx2")]
    pub unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        let mut i = 0;
        let len = a.len();
        let chunk_size = 8;

        while i + chunk_size <= len {
            let mut sum1 = _mm256_setzero_ps();
            let mut sum2 = _mm256_setzero_ps();

            for j in 0..chunk_size / 8 {
                let a_chunk = _mm256_loadu_ps(a.as_ptr().add(i + j * 8));
                let b_chunk = _mm256_loadu_ps(b.as_ptr().add(i + j * 8));
                let mul = _mm256_mul_ps(a_chunk, b_chunk);
                sum1 = _mm256_add_ps(sum1, mul);
            }

            let h_sum = _mm256_hadd_ps(sum1, sum2);
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
    pub unsafe fn dot_product_avx(a: &[f32], b: &[f32]) -> f32 {
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
    pub unsafe fn dot_product_sse(a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        let mut i = 0;
        let len = a.len();
        let chunk_size = 4;

        while i + chunk_size <= len {
            let a_chunk = _mm_loadu_ps(a.as_ptr().add(i));
            let b_chunk = _mm_loadu_ps(b.as_ptr().add(i));
            let mul = _mm_mul_ps(a_chunk, b_chunk);
            let h_sum = _mm_hadd_ps(mul, mul);
            let v_sum = _mm_hadd_ps(h_sum, h_sum);
            sum += _mm_cvtss_f32(v_sum);
            i += chunk_size;
        }

        for j in i..len {
            sum += a[j] * b[j];
        }

        sum
    }

    pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            panic!("Vectors must have the same length");
        }

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return unsafe { Self::dot_product_avx2(a, b) };
            } else if is_x86_feature_detected!("avx") {
                return unsafe { Self::dot_product_avx(a, b) };
            } else if is_x86_feature_detected!("sse") {
                return unsafe { Self::dot_product_sse(a, b) };
            }
        }

        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot = Self::dot_product(a, b);
        let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        dot / (norm_a * norm_b)
    }

    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        let mut sum = 0.0;
        for (x, y) in a.iter().zip(b.iter()) {
            let diff = x - y;
            sum += diff * diff;
        }
        sum.sqrt()
    }

    pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum()
    }
}

pub struct SimdVector {
    data: Vec<f32>,
}

impl SimdVector {
    pub fn new(data: Vec<f32>) -> Self {
        Self { data }
    }

    pub fn dot(&self, other: &Self) -> f32 {
        SimdOperations::dot_product(&self.data, &other.data)
    }

    pub fn cosine_similarity(&self, other: &Self) -> f32 {
        SimdOperations::cosine_similarity(&self.data, &other.data)
    }

    pub fn euclidean_distance(&self, other: &Self) -> f32 {
        SimdOperations::euclidean_distance(&self.data, &other.data)
    }

    pub fn manhattan_distance(&self, other: &Self) -> f32 {
        SimdOperations::manhattan_distance(&self.data, &other.data)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        let result = SimdOperations::dot_product(&a, &b);
        assert_eq!(result, 70.0);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let result = SimdOperations::cosine_similarity(&a, &b);
        assert_eq!(result, 1.0);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let result = SimdOperations::euclidean_distance(&a, &b);
        assert!((result - 5.196152).abs() < 1e-6);
    }
}
