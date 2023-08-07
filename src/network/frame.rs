mod frame;
pub use frame::FrameCoder;

use std::io::{Read, Write};
use crate::{CommandRequest, CommandResponse, KvError};
use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

pub const LEN_SIZE: usize  = 4;

const MAX_FRAME: usize = 2*1024*1024*1024;

const COMPRESSION_LIMIT: usize = 1436;

const COMPRESSION_BIT: usize = 1 << 31;

pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();

        if size > MAX_FRAME {
            return Err(KvError::FrameError);
        }

        buf.put_u32(size as _);

        if size > COMPRESSION_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;

            let payload = buf.split_off(LEN_SIZE);
            buf.clear();

            let mut encoder = GzEncoder::new(payload, Compression::default());
            encoder.write_all(&buf1[..])?;

            let payload = encoder.finish()?.into_inner();
            debug!("encode payload size: {}", payload.len());

            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);
            buf.unsplit(payload);
            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {

        let header = buf.get_u32() as usize;
        let (len,compressed) = decode_header(header);
        debug!("get a frame len: {}, compressed: {}", len, compressed);

        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len*2);
            decoder.read_to_end(&mut buf1)?;
            buf.advance(len);
            Ok(Self::decode(&buf1[..buf1.len()]))
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }  
}  

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

    

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use bytes::Bytes;

    fn command_request_encode_decode_should_work() {
        
    }
}