use warp::Filter;

pub mod auth {
    use super::*;

    pub fn with_auth() -> impl Filter<Extract = (String,), Error = warp::Rejection> + Clone {
        warp::header::optional::<String>("Authorization")
            .and_then(|auth_header: Option<String>| async move {
                match auth_header {
                    Some(header) if header.starts_with("Bearer ") => {
                        let token = header.trim_start_matches("Bearer ").to_string();
                        // Validate token (mock implementation)
                        if validate_token(&token) {
                            Ok(token)
                        } else {
                            Err(warp::reject::custom(AuthError::InvalidToken))
                        }
                    },
                    None => {
                        // Allow anonymous access for now
                        Ok("anonymous".to_string())
                    },
                    _ => {
                        Err(warp::reject::custom(AuthError::InvalidHeader))
                    },
                }
            })
    }

    fn validate_token(token: &str) -> bool {
        // Mock token validation
        !token.is_empty()
    }

    #[derive(Debug)]
    pub enum AuthError {
        InvalidToken,
        InvalidHeader,
    }

    impl std::fmt::Display for AuthError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                AuthError::InvalidToken => write!(f, "Invalid token"),
                AuthError::InvalidHeader => write!(f, "Invalid authorization header"),
            }
        }
    }

    impl std::error::Error for AuthError {}

    impl warp::reject::Reject for AuthError {}
}

pub mod cors {
    use super::*;

    pub fn with_cors() -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
        warp::cors()
            .allow_origins(vec!["*"])
            .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allow_headers(vec!["Content-Type", "Authorization"])
            .build()
    }
}

pub mod logging {
    use super::*;

    pub fn with_logging() -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
        warp::log::custom(|info| {
            eprintln!(
                "{} {} {} {}ms",
                info.method(),
                info.path(),
                info.status(),
                info.elapsed().as_millis()
            );
        })
    }
}