package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

@InterfaceAudience.Private
public class XNORErasureDecoder extends ErasureDecoder {

  public XNORErasureDecoder(ErasureCoderOptions options) {
    super(options);
  }

  @Override
  protected ErasureCodingStep prepareDecodingStep(
      final ECBlockGroup blockGroup) {
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(getConf(),
        ErasureCodeConstants.XNOR_CODEC_NAME, getOptions());

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureDecodingStep(inputBlocks,
        getErasedIndexes(inputBlocks),
        getOutputBlocks(blockGroup), rawDecoder);
  }

  @Override
  protected ECBlock[] getOutputBlocks(ECBlockGroup blockGroup) {

    int erasedNum = getNumErasedBlocks(blockGroup);
    ECBlock[] outputBlocks = new ECBlock[erasedNum];

    int idx = 0;
    for (int i = 0; i < getNumParityUnits(); i++) {
      if (blockGroup.getParityBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getParityBlocks()[i];
      }
    }

    for (int i = 0; i < getNumDataUnits(); i++) {
      if (blockGroup.getDataBlocks()[i].isErased()) {
        outputBlocks[idx++] = blockGroup.getDataBlocks()[i];
      }
    }

    return outputBlocks;
  }
}
