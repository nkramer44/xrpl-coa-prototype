use tokio_util::codec::LengthDelimitedCodec;

pub fn length_delimited_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder().max_frame_length(64 * 1024 * 1024).new_codec()
}