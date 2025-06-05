//registerProcessor("WasmProcessor", class WasmProcessor extends AudioWorkletProcessor {
//	constructor(options) {
//		super();
//		console.log(options)
//		let [module, memory, handle] = options;
//		bindgen.initSync({ module, memory });
//		this.processor = bindgen.WasmAudioProcessor.unpack(handle);
//		//this.port.onmessage = async (event) => {
//		//	if (event.data.type === "atten") {
//		//		const atten = event.data.atten;
//		//		this.processor.set_atten_lim(atten)
//		//	} else if (event.data.type === "init") {
//		//	}
//		//
//		//}
//	}
//	process(inputs, outputs) {
//		for (let i = 0; i < outputs[0].length; i++) {
//			outputs[0][i]
//				= this.processor.process(inputs[0][0]);
//		}
//		return true
//	}
//});

class WasmProcessor extends AudioWorkletProcessor {
    constructor(options) {
        super(options)
        const wasmBytes = options.processorOptions.wasmBytes;
        const modelBytes = new Uint8Array(options.processorOptions.modelBytes);
        const mod = new WebAssembly.Module(wasmBytes);
        this.wasm = new WebAssembly.Instance(mod, {});

        const modelPtr = this.wasm.exports.alloc(modelBytes.length)
        this.modelBytesCopy = new Uint8Array(this.wasm.exports.memory.buffer, 
            modelPtr, modelBytes.length
        )
        this.modelBytesCopy.set(modelBytes)

        this.inptr = this.wasm.exports.alloc(128 * 4); // float32 size: 4
        this.outptr = this.wasm.exports.alloc(128 * 4); // float32 size: 4
        this.dsp = this.wasm.exports.df_new(modelPtr, modelBytes.length, 30.0);
        this.inputbuf = new Float32Array(this.wasm.exports.memory.buffer,
            this.inptr,
            128
        )
        this.outbuf = new Float32Array(this.wasm.exports.memory.buffer,
            this.outptr,
            128);
    }

    process(inputs, outputs, parameters) {
        const output = outputs[0];
        this.inputs.set(inputs[0][0])
        this.wasm.exports.df_process(this.dsp, this.inptr, this.outptr, 128);
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

