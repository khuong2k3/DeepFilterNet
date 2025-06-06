//import * as df from './pkg/df'
//import { moveToWasm } from './wasm-util'
import { Model } from './noise-model'
import * as df from './pkg/df_audio_worklet'

export const AUDIO_WORKET_PROCESS_LENGTH = 128;

//class WasmProcessor extends AudioWorkletProcessor {
//    constructor(options) {
//        super(options)
//        //console.log(options)
//        const wasmBytes = options.processorOptions.wasmBytes;
//        const modelBytes = new Uint8Array(options.processorOptions.modelBytes);
//        const mod = new WebAssembly.Module(wasmBytes)
//        //console.log(df.)
//        //const wasm_df = new WebAssembly.Instance(mod, )
//        //console.log(mod, wasm_df)
//    }
//
//    process(inputs, outputs, parameters) {
//        return true;
//    }
//}

class WasmProcessor extends AudioWorkletProcessor {
    constructor(options) {
        super(options)
        const wasmBytes = options.processorOptions.wasmBytes;
        const modelBytes = new Uint8Array(options.processorOptions.modelBytes);
        const mod = new WebAssembly.Module(wasmBytes);
        this.wasm = df.initSync({module: mod})

        this.model = new Model(this.wasm, modelBytes, AUDIO_WORKET_PROCESS_LENGTH)
        this.model.process_frame(new Float32Array(this.model.frame_length()))

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
        //if (input.length === 128) {
        //    this.inputbuf.set(input[0])
        //} else {
        //    this.inputbuf.set(output[0])
        //}
        //
        //this.outbuf.set(output[0])
        //const outbuf = this.model.process_frame(output)
        ////console.log(this.outbuf)
        //this.wasm.df_process(this.dsp, this.inputbuf.length, this.inptr, this.outbuf.length, this.outptr);
        const outbuf = this.model.process_frame(output)
        for (let channel = 0; channel < output.length; ++channel) {
            output[channel].set(
                outbuf
                //this.outbuf
            )
        }

        return true;
    }
}

registerProcessor('WasmProcessor', WasmProcessor);

