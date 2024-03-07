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
    super::{
        state::{DecodeState, MetaState, RState, ResponseState},
        Decoder, ProtocolError,
    },
    crate::response::Response,
};

#[derive(Debug, PartialEq, Default)]
pub(crate) struct MRespState {
    processed: Vec<Response>,
    pending: Option<ResponseState>,
    expected: MetaState,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PipelineResult {
    Completed(Vec<Response>),
    Pending(MRespState),
    Error(ProtocolError),
}

impl MRespState {
    fn step(mut self, decoder: &mut Decoder) -> PipelineResult {
        match self.expected.finished(decoder) {
            Ok(true) => {}
            Ok(false) => return PipelineResult::Pending(self),
            Err(e) => return PipelineResult::Error(e),
        }
        loop {
            if self.processed.len() as u64 == self.expected.val() {
                return PipelineResult::Completed(self.processed);
            }
            match decoder.validate_response(RState(
                self.pending.take().unwrap_or(ResponseState::Initial),
            )) {
                DecodeState::ChangeState(RState(s)) => {
                    self.pending = Some(s);
                    return PipelineResult::Pending(self);
                }
                DecodeState::Completed(c) => self.processed.push(c),
                DecodeState::Error(e) => return PipelineResult::Error(e),
            }
        }
    }
}

impl<'a> Decoder<'a> {
    pub fn validate_pipe(&mut self, first: bool, state: MRespState) -> PipelineResult {
        if first && self._cursor_next() != b'P' {
            PipelineResult::Error(ProtocolError::InvalidPacket)
        } else {
            state.step(self)
        }
    }
}

#[test]
fn t_pipe() {
    use crate::response::{Response, Row, Value};
    let mut decoder = Decoder::new(b"P5\n\x12\x10\xFF\xFF\x115\n\x00\x01\x01\x0D5\nsayan\x0220\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nelana\x0221\n\x0E0\n\x115\n\x00\x01\x01\x0D5\nemily\x0222\n\x0E0\n", 0);
    assert_eq!(
        decoder.validate_pipe(true, MRespState::default()),
        PipelineResult::Completed(vec![
            Response::Empty,
            Response::Error(u16::MAX),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("sayan".into()),
                Value::UInt8(20),
                Value::List(vec![])
            ])),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("elana".into()),
                Value::UInt8(21),
                Value::List(vec![])
            ])),
            Response::Row(Row::new(vec![
                Value::Null,
                Value::Bool(true),
                Value::String("emily".into()),
                Value::UInt8(22),
                Value::List(vec![])
            ]))
        ])
    );
}
