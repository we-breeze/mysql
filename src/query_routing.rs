//! Optional per-handle routing policy and explicit key.

use std::sync::Arc;

use crate::{
    MysqlArgs, MysqlResult, MysqlRouteKey, MysqlRouting,
    arguments::ArgumentRouteKey,
    routing::{RouteRenderer, invalid},
};

#[derive(Clone)]
pub(crate) struct QueryRouting {
    policy: Arc<dyn RouteRenderer>,
    key: Option<Arc<dyn MysqlRouteKey>>,
}

impl QueryRouting {
    pub(crate) fn new<R: MysqlRouting + 'static>(policy: R) -> Self {
        Self {
            policy: Arc::new(policy),
            key: None,
        }
    }

    pub(crate) fn with_key<K: MysqlRouteKey>(&self, key: K) -> Self {
        Self {
            policy: self.policy.clone(),
            key: Some(Arc::new(key)),
        }
    }

    pub(crate) fn render_template<A: MysqlArgs>(
        &self,
        template: &str,
        arguments: &A,
        implicit_key: &mut Option<ArgumentRouteKey>,
        out: &mut dyn std::fmt::Write,
    ) -> MysqlResult<()> {
        let key = match &self.key {
            Some(key) => key.as_ref(),
            None => {
                if implicit_key.is_none() {
                    let value = arguments.first_route_value().ok_or_else(|| {
                        invalid("sharded queries require .route(key) or a first SQL argument")
                    })?;
                    *implicit_key = Some(ArgumentRouteKey::new(value)?);
                }
                implicit_key.as_ref().unwrap().as_key()
            }
        };
        self.policy.render(template, key, out)
    }
}

impl std::fmt::Debug for QueryRouting {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryRouting")
            .field("has_explicit_key", &self.key.is_some())
            .finish_non_exhaustive()
    }
}
