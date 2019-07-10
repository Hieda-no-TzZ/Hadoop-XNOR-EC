package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import java.nio.ByteBuffer;

@InterfaceAudience.Private
public class XNORRawEncoder extends RawErasureEncoder {

  public XNORRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
  }

  protected void doEncode(ByteBufferEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);
    ByteBuffer output = encodingState.outputs[0];

    // Get the first buffer's data.
    int iIdx, oIdx;
    for (iIdx = encodingState.inputs[0].position(), oIdx = output.position();
         iIdx < encodingState.inputs[0].limit(); iIdx++, oIdx++) {
      output.put(oIdx, encodingState.inputs[0].get(iIdx));
    }

    // XNOR with everything else.
    for (int i = 1; i < encodingState.inputs.length; i++) {
      for (iIdx = encodingState.inputs[i].position(), oIdx = output.position();
           iIdx < encodingState.inputs[i].limit();
           iIdx++, oIdx++) {
        output.put(oIdx, (byte) (0xff ^ output.get(oIdx) ^ encodingState.inputs[i].get(iIdx)));
      }
    }
  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) {
    int dataLen = encodingState.encodeLength;
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets, dataLen);
    byte[] output = encodingState.outputs[0];

    // Get the first buffer's data.
    int iIdx, oIdx;
    for (iIdx = encodingState.inputOffsets[0], oIdx = encodingState.outputOffsets[0];
         iIdx < encodingState.inputOffsets[0] + dataLen; iIdx++, oIdx++) {
      output[oIdx] = encodingState.inputs[0][iIdx];
    }

    // XNOR with everything else.
    for (int i = 1; i < encodingState.inputs.length; i++) {
      for (iIdx = encodingState.inputOffsets[i],
               oIdx = encodingState.outputOffsets[0];
           iIdx < encodingState.inputOffsets[i] + dataLen; iIdx++, oIdx++) {
        output[oIdx] ^= encodingState.inputs[i][iIdx] ^ 0xff;
      }
    }
  }
}
