package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * A raw coder factory for raw XNOR coder.
 */
@InterfaceAudience.Private
public class XNORRawErasureCoderFactory implements RawErasureCoderFactory {

  public static final String CODER_NAME = "xnor_java";

  @Override
  public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
    return new XNORRawEncoder(coderOptions);
  }

  @Override
  public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
    return new XNORRawDecoder(coderOptions);
  }

  @Override
  public String getCoderName() {
    return CODER_NAME;
  }

  @Override
  public String getCodecName() {
    return ErasureCodeConstants.XNOR_CODEC_NAME;
  }
}
