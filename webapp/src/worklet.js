import * as df from './pkg/df_audio_worklet'

class WasmProcessor extends AudioWorkletProcessor {
    constructor(options) {
        super(options)
        const wasmBytes = options.processorOptions.wasmBytes;
        const modelBytes = new Uint8Array(options.processorOptions.modelBytes);
        const mod = new WebAssembly.Module(wasmBytes);
        this.wasm = df.initSync({module: mod})

        const wasmHeap = this.wasm.alloc(modelBytes.length + 128 * 4 + 128 * 4)

        const modelPtr = wasmHeap
        let modelBytesCopy = new Uint8Array(this.wasm.memory.buffer, 
            wasmHeap, modelBytes.length
        )
        modelBytesCopy.set(modelBytes)

        this.inptr = wasmHeap + modelBytes.length
            //this.wasm.alloc(128 * 4); // float32 size: 4
        this.outptr = this.inptr + 128 * 4
            //this.wasm.alloc(128 * 4); // float32 size: 4
        //console.log('not created', modelBytesCopy, modelBytesCopy.length)
        //console.log(modelPtr, modelBytesCopy.length)
        this.dsp = this.wasm.df_new(modelPtr, modelBytesCopy.length, 30.0);

        this.inputbuf = new Float32Array(this.wasm.memory.buffer,
            this.inptr,
            128
        )
        this.outbuf = new Float32Array(this.wasm.memory.buffer,
            this.outptr,
            128);
    }

    process(inputs, outputs, parameters) {
        const output = outputs[0];
        this.inputbuf.set(output[0])
        this.outbuf.set(output[0])

        //this.wasm.df_process(this.dsp, this.inptr, 128, this.outptr, 128);
        this.wasm.df_process(this.dsp, this.inputbuf.length, this.inptr,this.outbuf.length,  this.outptr);
        //this.wasm.df_set_atten_lim(this.dsp, 30.0);
        for (let channel = 0; channel < output.length; ++channel) {
            const outputChannel = output[channel];
            for (let i = 0; i < outputChannel.length; ++i) {
                outputChannel[i] = this.outbuf[i];
            }
        }

        return true;
    }
}

registerProcessor('WasmProcessor', WasmProcessor);

