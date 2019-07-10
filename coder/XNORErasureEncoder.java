package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

@InterfaceAudience.Private
public class XNORErasureEncoder extends ErasureEncoder {

  public XNORErasureEncoder(ErasureCoderOptions options) {
    super(options);
  }

  @Override
  protected ErasureCodingStep prepareEncodingStep(
      final ECBlockGroup blockGroup) {
    RawErasureEncoder rawEncoder = CodecUtil.createRawEncoder(getConf(),
        ErasureCodeConstants.XNOR_CODEC_NAME, getOptions());

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureEncodingStep(inputBlocks,
        getOutputBlocks(blockGroup), rawEncoder);
  }
}
