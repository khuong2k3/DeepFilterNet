import * as df from './pkg/df_audio_worklet'

export function moveToWasm(wasm: df.InitOutput, array: ArrayBuffer): [ArrayBuffer, number] {
    const arrayUint = new Uint8Array(array)
    const valuePtr = wasm.alloc(arrayUint.length)
    let copyArray = new Uint8Array(wasm.memory.buffer, valuePtr, arrayUint.length)
    copyArray.set(arrayUint)
    //console.log('datacopy: ', copyArray)

    return [copyArray.buffer, valuePtr]
}

export function allocWasm(wasm: df.InitOutput, length: number): [ArrayBuffer, number] {
    const allocPtr = wasm.alloc(length)

    return [wasm.memory.buffer.slice(allocPtr, allocPtr + length), allocPtr]
}


export async function fetchModel(modelUrl: string) {
  const response = await fetch(modelUrl);
  //const response = await fetch("/DeepFilterNet3_ll_onnx.tar.gz");
  const arrayBuffer = await response.arrayBuffer()
  return arrayBuffer
}

export async function setupAudioWorklet(modelUrl: string, audioCtx: AudioContext) {
  const worketURL = new URL('worklet.js', import.meta.url)
  const wasmURL = new URL('./pkg/df_audio_worklet_bg.wasm', import.meta.url)

  const [modelArrayTar, wasmRes, _] = await Promise.all([
    //fetchModel("/DeepFilterNet3_onnx.tar.gz"),
    fetchModel(modelUrl),
    fetch(wasmURL),
    audioCtx.audioWorklet.addModule(worketURL)
  ])

  const modelBytes = new Uint8Array(modelArrayTar)

  const wasmBuffer = await wasmRes.arrayBuffer()
  const wasmBytes = new Uint8Array(wasmBuffer)

  const audioNode = new AudioWorkletNode(audioCtx, 'WasmProcessor', {
    processorOptions: {
      wasmBytes, modelBytes
    }
  })

  return audioNode
}

export function writeWav(audio: AudioBuffer) {
    const numChannels = 1
    const bytesPerSample = 2 * numChannels;
    const bytesPerSecond = audio.sampleRate * bytesPerSample;
    const dataLength = bytesPerSecond * audio.duration;
    //const dataLength = audio.length * 2 
    const headerLength = 44;
    const fileLength = dataLength + headerLength;
    const bufferData = new Uint8Array(fileLength);
    const dataView = new DataView(bufferData.buffer);
    const writer = createWriter(dataView);
    writer.string("RIFF");
    // File Size
    writer.uint32(fileLength);
    writer.string("WAVE");

    writer.string("fmt ");
    // Chunk Size
    writer.uint32(16);
    // Format Tag
    writer.uint16(1);
    // Number Channels
    writer.uint16(numChannels);
    // Sample Rate
    writer.uint32(audio.sampleRate);
    // Bytes Per Second
    writer.uint32(bytesPerSecond);
    // Bytes Per Sample
    writer.uint16(bytesPerSample);
    // Bits Per Sample
    writer.uint16(bytesPerSample * 8);
    writer.string("data");

    writer.uint32(dataLength);

    const audioData = audio.getChannelData(0)
    for (let i = 0; i < dataLength / 2; i++) {
        writer.pcm16s(audioData[i]);
    }

    return dataView.buffer
}

//const sampleRate = 8000;
//const durationSeconds = 10;
//const numChannels = 1;
//const bytesPerSample = 2 * numChannels;
//const bytesPerSecond = sampleRate * bytesPerSample;
//const dataLength = bytesPerSecond * durationSeconds;
//const headerLength = 44;
//const fileLength = dataLength + headerLength;
//const bufferData = new Uint8Array(fileLength);
//const dataView = new DataView(bufferData.buffer);
//const writer = createWriter(dataView);
//
//// HEADER
//writer.string("RIFF");
//// File Size
//writer.uint32(fileLength);
//writer.string("WAVE");
//
//writer.string("fmt ");
//// Chunk Size
//writer.uint32(16);
//// Format Tag
//writer.uint16(1);
//// Number Channels
//writer.uint16(numChannels);
//// Sample Rate
//writer.uint32(sampleRate);
//// Bytes Per Second
//writer.uint32(bytesPerSecond);
//// Bytes Per Sample
//writer.uint16(bytesPerSample);
//// Bits Per Sample
//writer.uint16(bytesPerSample * 8);
//writer.string("data");
//
//writer.uint32(dataLength);
//
//for (let i = 0; i < dataLength / 2; i++) {
//    const t = i / sampleRate;
//    const frequency = 256;
//    const volume = 0.6;
//    const val = Math.sin(2 * Math.PI * 256 * t) * volume;
//    writer.pcm16s(val);
//}

//const blob = new Blob([dataView.buffer], { type: 'application/octet-stream' });
//audioPlayer.src = URL.createObjectURL(blob);

function createWriter(dataView) {
    let pos = 0;

    return {
        string(val) {
            for (let i = 0; i < val.length; i++) {
                dataView.setUint8(pos++, val.charCodeAt(i));
            }
        },
        uint16(val) {
            dataView.setUint16(pos, val, true);
            pos += 2;
        },
        uint32(val) {
            dataView.setUint32(pos, val, true);
            pos += 4;
        },
        pcm16s: function(value) {
            value = Math.round(value * 32768);
            value = Math.max(-32768, Math.min(value, 32767));
            dataView.setInt16(pos, value, true);
            pos += 2;
        },
    } as const
}





