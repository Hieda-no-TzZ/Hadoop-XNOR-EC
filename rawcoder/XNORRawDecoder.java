package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.nio.ByteBuffer;

@InterfaceAudience.Private
public class XNORRawDecoder extends RawErasureDecoder {

  public XNORRawDecoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
  }

  @Override
  protected void doDecode(ByteBufferDecodingState decodingState) {
    CoderUtil.resetOutputBuffers(decodingState.outputs,
        decodingState.decodeLength);
    ByteBuffer output = decodingState.outputs[0];

    int erasedIdx = decodingState.erasedIndexes[0];

    // Process the inputs.
    int iIdx, oIdx;
    boolean flag = true;
    for (int i = 0; i < decodingState.inputs.length; i++) {
      // Skip the erased location.
      if (i == erasedIdx) {
        continue;
      }

      for (iIdx = decodingState.inputs[i].position(), oIdx = output.position();
           iIdx < decodingState.inputs[i].limit();
           iIdx++, oIdx++) {
        if (flag) {
          output.put(oIdx, (byte) (0xff));
        }
        output.put(oIdx, (byte) (0xff ^ output.get(oIdx) ^ decodingState.inputs[i].get(iIdx)));
      }
      flag = false;
    }
  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) {
    byte[] output = decodingState.outputs[0];
    int dataLen = decodingState.decodeLength;
    CoderUtil.resetOutputBuffers(decodingState.outputs,
        decodingState.outputOffsets, dataLen);
    int erasedIdx = decodingState.erasedIndexes[0];

    // Process the inputs.
    int iIdx, oIdx;
    boolean flag = true;
    for (int i = 0; i < decodingState.inputs.length; i++) {
      // Skip the erased location.
      if (i == erasedIdx) {
        continue;
      }

      for (iIdx = decodingState.inputOffsets[i], oIdx = decodingState.outputOffsets[0];
           iIdx < decodingState.inputOffsets[i] + dataLen; iIdx++, oIdx++) {
        if (flag) {
          output[oIdx] = (byte) (0xff);
        }
        output[oIdx] ^= decodingState.inputs[i][iIdx] ^ 0xff;
      }
      flag = false;
    }
  }

}
