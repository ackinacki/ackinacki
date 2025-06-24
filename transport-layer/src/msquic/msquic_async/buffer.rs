use std::io::IoSlice;
use std::ops::Range;
use std::slice;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use libc::c_void;
use tracing::trace;

use super::stream::StreamInstance;

/// A buffer for receiving data from a stream.
///
/// It implements [`bytes::Buf`] and is backed by a list of [`msquic::ffi::QUIC_BUFFER`].
pub struct StreamRecvBuffer {
    stream: Option<Arc<StreamInstance>>,
    buffers: Vec<msquic::ffi::QUIC_BUFFER>,
    offset: usize,
    len: usize,
    read_cursor: usize,
    read_cursor_in_buffer: usize,
    fin: bool,
}

impl StreamRecvBuffer {
    pub(crate) fn new<T: AsRef<[msquic::BufferRef]> + ?Sized>(
        offset: usize,
        buffers: &T,
        fin: bool,
    ) -> Self {
        let buf = Self {
            stream: None,
            buffers: buffers.as_ref().iter().map(|v| v.0).collect(),
            offset,
            len: buffers.as_ref().iter().map(|x| x.0.Length).sum::<u32>() as usize,
            read_cursor: 0,
            read_cursor_in_buffer: 0,
            fin,
        };
        trace!(
            "StreamRecvBuffer({:p}) created offset={} len={} fin={}",
            buf.buffers.first().map(|x| x.Buffer).unwrap_or(std::ptr::null_mut()),
            buf.offset,
            buf.len(),
            buf.fin,
        );
        buf
    }

    pub(crate) fn set_stream(&mut self, stream: Arc<StreamInstance>) {
        trace!(
            "StreamRecvBuffer({:p}) set StreamInner({:p})",
            self.buffers.first().map(|x| x.Buffer).unwrap_or(std::ptr::null_mut()),
            stream
        );

        self.stream = Some(stream);
    }

    /// Returns the length of the buffer.
    pub fn len(&self) -> usize {
        if self.buffers.len() <= self.read_cursor {
            return 0;
        }
        self.len
            - self.buffers[..self.read_cursor].iter().map(|x| x.Length).sum::<u32>() as usize
            - self.read_cursor_in_buffer
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the buffer as a slice.
    pub fn as_slice_upto_size(&self, size: usize) -> &[u8] {
        if self.buffers.len() <= self.read_cursor {
            return &[];
        }
        assert!(self.buffers.len() >= self.read_cursor);
        let buffer = &self.buffers[self.read_cursor];
        assert!(buffer.Length as usize >= self.read_cursor_in_buffer);
        let len = std::cmp::min(buffer.Length as usize - self.read_cursor_in_buffer, size);
        unsafe { slice::from_raw_parts(buffer.Buffer.add(self.read_cursor_in_buffer), len) }
    }

    /// Consumes and returns the buffer as a slice.
    pub fn get_bytes_upto_size<'a>(&mut self, size: usize) -> Option<&'a [u8]> {
        if self.buffers.len() <= self.read_cursor {
            return None;
        }
        assert!(self.buffers.len() >= self.read_cursor);
        let buffer = &self.buffers[self.read_cursor];

        assert!(buffer.Length as usize >= self.read_cursor_in_buffer);
        let len = std::cmp::min(buffer.Length as usize - self.read_cursor_in_buffer, size);

        let slice =
            unsafe { slice::from_raw_parts(buffer.Buffer.add(self.read_cursor_in_buffer), len) };
        self.read_cursor_in_buffer += len;
        if self.read_cursor_in_buffer >= buffer.Length as usize {
            self.read_cursor += 1;
            self.read_cursor_in_buffer = 0;
        }
        Some(slice)
    }

    /// Return the offset in the stream.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Return the range in the stream.
    pub fn range(&self) -> Range<usize> {
        self.offset..self.offset + self.len
    }

    /// Return `true` if the buffer is the end of the stream.
    pub fn fin(&self) -> bool {
        self.fin
    }
}

unsafe impl Sync for StreamRecvBuffer {}
unsafe impl Send for StreamRecvBuffer {}

impl Buf for StreamRecvBuffer {
    fn advance(&mut self, mut count: usize) {
        assert!(count == 0 || count <= self.remaining());
        for buffer in &self.buffers[self.read_cursor..] {
            if count == 0 {
                break;
            }
            let remaining = buffer.Length as usize - self.read_cursor_in_buffer;
            if count < remaining {
                self.read_cursor_in_buffer += count;
                break;
            } else {
                self.read_cursor += 1;
                self.read_cursor_in_buffer = 0;
                count -= remaining;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        self.as_slice_upto_size(self.len())
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut count = 0;
        let mut read_cursor_in_buffer = Some(self.read_cursor_in_buffer);
        for buffer in &self.buffers[self.read_cursor..] {
            if let Some(slice) = dst.get_mut(count) {
                count += 1;
                let skip = read_cursor_in_buffer.take().unwrap_or(0);
                *slice = IoSlice::new(unsafe {
                    slice::from_raw_parts(buffer.Buffer.add(skip), buffer.Length as usize - skip)
                });
            } else {
                break;
            }
        }
        count
    }
}

impl Drop for StreamRecvBuffer {
    fn drop(&mut self) {
        trace!(
            "StreamRecvBuffer({:p}) dropping",
            self.buffers.first().map(|x| x.Buffer).unwrap_or(std::ptr::null_mut())
        );
        if let Some(stream) = self.stream.take() {
            stream.read_complete(self);
        }
    }
}

pub(crate) struct WriteBuffer(Box<WriteBufferInner>);

struct WriteBufferInner {
    internal: Vec<u8>,
    zerocopy: Vec<Bytes>,
    buffers: Vec<msquic::BufferRef>,
}
unsafe impl Sync for WriteBufferInner {}
unsafe impl Send for WriteBufferInner {}

impl WriteBuffer {
    pub(crate) fn new() -> Self {
        Self(Box::new(WriteBufferInner {
            internal: Vec::new(),
            zerocopy: Vec::new(),
            buffers: Vec::new(),
        }))
    }

    pub(crate) unsafe fn from_raw(inner: *const c_void) -> Self {
        Self(unsafe { Box::from_raw(inner as *mut WriteBufferInner) })
    }

    pub(crate) fn put_zerocopy(&mut self, buf: &Bytes) -> usize {
        self.0.zerocopy.push(buf.clone());
        buf.len()
    }

    pub(crate) fn put_slice(&mut self, slice: &[u8]) -> usize {
        self.0.internal.extend_from_slice(slice);
        slice.len()
    }

    pub(crate) fn get_buffers(&mut self) -> (*const msquic::BufferRef, usize) {
        if !self.0.zerocopy.is_empty() {
            for buf in &self.0.zerocopy {
                self.0.buffers.push(buf[..].into());
            }
        } else {
            self.0.buffers.push((&self.0.internal[..]).into());
        }
        let ptr = self.0.buffers.as_ptr();
        let len = self.0.buffers.len();
        (ptr, len)
    }

    pub(crate) fn into_raw(self) -> *mut c_void {
        Box::into_raw(self.0) as *mut c_void
    }

    pub(crate) fn reset(&mut self) {
        self.0.internal.clear();
        self.0.zerocopy.clear();
        self.0.buffers.clear();
    }
}
