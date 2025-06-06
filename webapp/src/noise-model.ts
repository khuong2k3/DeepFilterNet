import * as df from './pkg/df_audio_worklet'
import { allocWasm, moveToWasm } from './wasm-util'

export const FLOAT_SIZE = 4

export class Model {
    model: number
    wasm_df: df.InitOutput
    frame_size: number 
    frame_ceil: number

    inputPtr: number
    outPtr: number
    inputBuf: Float32Array
    outBuf: Float32Array

    processOffset: number
    readOffset: number
    process_frame_size: number

    constructor(wasm_df: df.InitOutput, modelBytes: Uint8Array, process_frame_size: number) {
        const [modelBytesCopy, modelPtr] = moveToWasm(wasm_df, modelBytes)
        //const modelBytesCopy = new Uint8Array(modelArrayCopy)
        //console.log('model: ', modelBytesCopy.byteLength, modelPtr)
        //console.log("org: ", modelBytes)

        this.wasm_df = wasm_df
        this.model = this.wasm_df.df_new(modelPtr, modelBytesCopy.byteLength, 20.0)
        this.frame_size = this.wasm_df.frame_size(this.model)
        console.log('model: ', modelBytesCopy, modelPtr)

        this.process_frame_size = Math.min(process_frame_size, this.frame_size)
        this.frame_ceil = Math.ceil(this.frame_size / this.process_frame_size) * this.process_frame_size
        //const wasmHeap = this.wasm_df.alloc(this.frame_ceil * FLOAT_SIZE * 2)
        const heapWasm = this.wasm_df.alloc(this.frame_size * FLOAT_SIZE + this.frame_ceil * FLOAT_SIZE)
        //const [inputBuf, inputPtr] = allocWasm(this.wasm_df, this.frame_size * FLOAT_SIZE)
        //const [outBuf, outPtr] = allocWasm(this.wasm_df, this.frame_ceil * FLOAT_SIZE)
        //console.log(this.frame_ceil, this.frame_size)

        //console.log(inputBuf.byteLength, outBuf.byteLength)
        this.inputPtr = heapWasm
        this.inputBuf = new Float32Array(this.wasm_df.memory.buffer, this.inputPtr, this.frame_size)
        this.outPtr = this.inputPtr + this.frame_size * FLOAT_SIZE 
        this.outBuf = new Float32Array(this.wasm_df.memory.buffer, this.outPtr, this.frame_ceil)
        this.processOffset = 0
        this.readOffset = 0

        this.process_frame(new Float32Array(this.frame_size))
    }

    //process_frame(input: Float32Array) { 
    //    this.inputBuf.set(input.slice(0, this.frame_size))
    //
    //    this.wasm_df.df_process(this.model, this.inputPtr, this.frame_size, this.outPtr, this.frame_size)
    //
    //    let output = new Float32Array(this.frame_size)
    //    output.set(this.outBuf)
    //
    //    return output
    //}

    process_frame(input: Float32Array) { 
        const oldOffset = this.processOffset
        const moveOffset = Math.min(input.length, this.frame_size - oldOffset)

        this.inputBuf.set(input.slice(0, moveOffset), this.processOffset)
        if (this.processOffset + moveOffset >= this.frame_size - 1) {
            this.processOffset = this.processOffset + moveOffset

            const inputLeftOver = input.slice(this.frame_size)
            this.processOffset = Math.min(inputLeftOver.length, this.frame_ceil - this.frame_size)

            console.assert(this.processOffset + this.frame_size === this.frame_ceil)

            this.wasm_df.df_process(this.model, this.inputPtr, this.frame_size, this.outPtr + this.processOffset * FLOAT_SIZE, this.frame_size)

            this.inputBuf.set(inputLeftOver.slice(0, this.processOffset))
        } else {
            this.processOffset = this.processOffset + moveOffset
        }
        const startReadOffset = this.readOffset
        const endReadOffset = startReadOffset + this.process_frame_size 
        this.readOffset += this.process_frame_size 

        if (this.readOffset >= this.frame_ceil) {
            //console.log(this.readOffset, startReadOffset, endReadOffset)
            this.readOffset = 0
        }
        //let output = new Float32Array(this.process_frame_size)
        //output.set(this.outBuf.slice(startReadOffset, endReadOffset))
        //console.log(this.outBuf)

        //console.log(startReadOffset, endReadOffset)
        //return output
        return this.outBuf.slice(startReadOffset, endReadOffset)
    }

    frame_length() {
        return this.frame_size
    }
}
