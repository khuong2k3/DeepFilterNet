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
        this.outptr = this.inptr + 128 * 4
        this.dsp = this.wasm.df_new(modelPtr, modelBytesCopy.length, 30.0);

        this.inputbuf = new Float32Array(this.wasm.memory.buffer,
            this.inptr,
            128
        )
        this.outbuf = new Float32Array(this.wasm.memory.buffer,
            this.outptr,
            128);

        this.port.onmessage = async (event) => {
            if (event.data.type === 'atten') {
                const atten = event.data.atten
                df.df_set_atten_lim(this.dsp, atten)
            }
            console.log(data)
        }
    }

    process(inputs, outputs, parameters) {
        const input = inputs[0]
        const output = outputs[0];
        if (input.length === 128) {
            this.inputbuf.set(input[0])
        } else {
            this.inputbuf.set(output[0])
        }
        this.outbuf.set(output[0])

        this.wasm.df_process(this.dsp, this.inputbuf.length, this.inptr, this.outbuf.length, this.outptr);
        for (let channel = 0; channel < output.length; ++channel) {
            output[channel].set(
                this.outbuf
            )
            //for (let i = 0; i < output[channel].length; ++i) {
            //    output[channel][i] = 0.01 * (2 * Math.random() - 1)
            //}
        }

        return true;
    }
}

registerProcessor('WasmProcessor', WasmProcessor);

