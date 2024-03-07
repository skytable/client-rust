/*
 * Copyright 2024, Sayan Nandan <nandansayan@outlook.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

use {
    super::{Decoder, ProtocolError, ProtocolResult},
    crate::response::{Response, Row, Value},
};

pub type ValueDecodeStateRaw = ValueDecodeStateAny<ValueState>;
pub type ValueDecodeState = ValueDecodeStateAny<PendingValue>;

/*
    pending value
    ---
    a stack is useful for recursive types
*/

#[derive(Debug, PartialEq)]
pub struct PendingValue {
    pub(super) state: ValueState,
    pub(super) tmp: Option<ValueState>,
    pub(super) stack: Vec<(Vec<Value>, ValueStateMeta)>,
}

impl PendingValue {
    pub fn new(
        state: ValueState,
        tmp: Option<ValueState>,
        stack: Vec<(Vec<Value>, ValueStateMeta)>,
    ) -> Self {
        Self { state, tmp, stack }
    }
}

/*
    value state
*/

#[derive(Debug, PartialEq)]
pub enum ValueDecodeStateAny<P, V = Value> {
    Pending(P),
    Decoded(V),
}

#[derive(Debug, PartialEq)]
pub struct ValueState {
    pub(super) v: Value,
    pub(super) meta: ValueStateMeta,
}

impl ValueState {
    pub fn new(v: Value, meta: ValueStateMeta) -> Self {
        Self { v, meta }
    }
}

#[derive(Debug, PartialEq)]
pub struct ValueStateMeta {
    pub(super) start: usize,
    pub(super) md: MetaState,
}

impl ValueStateMeta {
    pub fn zero() -> Self {
        Self {
            start: 0,
            md: MetaState::default(),
        }
    }
    pub fn new(start: usize, md1: u64, md1_flag: bool) -> Self {
        Self {
            start,
            md: MetaState::new(md1_flag, md1),
        }
    }
}

/*
    metadata init state
*/

#[derive(Debug, Default, PartialEq)]
pub struct MetaState {
    completed: bool,
    val: u64,
}

impl MetaState {
    pub fn new(completed: bool, val: u64) -> Self {
        Self { completed, val }
    }
    #[inline(always)]
    pub fn finished(&mut self, decoder: &mut Decoder) -> ProtocolResult<bool> {
        self.finish_or_continue(decoder, || Ok(true), || Ok(false), |e| Err(e))
    }
    #[inline(always)]
    pub fn finish_or_continue<T>(
        &mut self,
        decoder: &mut Decoder,
        if_completed: impl FnOnce() -> T,
        if_pending: impl FnOnce() -> T,
        if_err: impl FnOnce(ProtocolError) -> T,
    ) -> T {
        Self::try_finish_or_continue(
            self.completed,
            &mut self.val,
            decoder,
            if_completed,
            if_pending,
            if_err,
        )
    }
    #[inline(always)]
    pub fn try_finish(
        decoder: &mut Decoder,
        completed: bool,
        val: &mut u64,
    ) -> ProtocolResult<bool> {
        Self::try_finish_or_continue(
            completed,
            val,
            decoder,
            || Ok(true),
            || Ok(false),
            |e| Err(e),
        )
    }
    #[inline(always)]
    pub fn try_finish_or_continue<T>(
        completed: bool,
        val: &mut u64,
        decoder: &mut Decoder,
        if_completed: impl FnOnce() -> T,
        if_pending: impl FnOnce() -> T,
        if_err: impl FnOnce(ProtocolError) -> T,
    ) -> T {
        if completed {
            if_completed()
        } else {
            match decoder.__resume_decode(*val, ValueStateMeta::zero()) {
                Ok(vs) => match vs {
                    ValueDecodeStateAny::Pending(ValueState { v, .. }) => {
                        *val = v.u64();
                        if_pending()
                    }
                    ValueDecodeStateAny::Decoded(v) => {
                        *val = v.u64();
                        if_completed()
                    }
                },
                Err(e) => if_err(e),
            }
        }
    }
    #[inline(always)]
    pub fn val(&self) -> u64 {
        self.val
    }
}

/*
    row state
*/

#[derive(Debug, PartialEq)]
pub struct RowState {
    pub(super) meta: ValueStateMeta,
    pub(super) row: Vec<Value>,
    pub(super) tmp: Option<PendingValue>,
}

impl RowState {
    pub fn new(meta: ValueStateMeta, row: Vec<Value>, tmp: Option<PendingValue>) -> Self {
        Self { meta, row, tmp }
    }
}

/*
    multi row state
*/

#[derive(Debug, PartialEq)]
pub struct MultiRowState {
    pub(super) c_row: Option<RowState>,
    pub(super) rows: Vec<Row>,
    pub(super) md_state: u8,
    pub(super) md1_target: u64,
    pub(super) md2_col_cnt: u64,
}

impl Default for MultiRowState {
    fn default() -> Self {
        Self::new(None, vec![], 0, 0, 0)
    }
}

impl MultiRowState {
    pub fn new(
        c_row: Option<RowState>,
        rows: Vec<Row>,
        md_s: u8,
        md_cnt: u64,
        md_target: u64,
    ) -> Self {
        Self {
            c_row,
            rows,
            md_state: md_s,
            md1_target: md_target,
            md2_col_cnt: md_cnt,
        }
    }
}

/*
    response state
*/

#[derive(Debug, PartialEq)]
pub enum ResponseState {
    Initial,
    PValue(PendingValue),
    PError,
    PRow(RowState),
    PMultiRow(MultiRowState),
}

#[derive(Debug, PartialEq)]
pub enum DecodeState {
    ChangeState(RState),
    Completed(Response),
    Error(ProtocolError),
}

#[derive(Debug, PartialEq)]
pub struct RState(pub(super) ResponseState);
impl Default for RState {
    fn default() -> Self {
        RState(ResponseState::Initial)
    }
}
