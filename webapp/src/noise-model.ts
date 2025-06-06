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

    outRingBuf: RingBuffer
    processOffset: number
    readOffset: number
    process_frame_size: number
    prepareOutputFrame: Float32Array

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
        const heapWasm = this.wasm_df.alloc((this.frame_size + this.frame_size) * FLOAT_SIZE)
        //const [inputBuf, inputPtr] = allocWasm(this.wasm_df, this.frame_size * FLOAT_SIZE)
        //const [outBuf, outPtr] = allocWasm(this.wasm_df, this.frame_ceil * FLOAT_SIZE)
        //console.log(this.frame_ceil, this.frame_size)

        //console.log(inputBuf.byteLength, outBuf.byteLength)
        this.inputPtr = heapWasm
        this.inputBuf = new Float32Array(this.wasm_df.memory.buffer, this.inputPtr, this.frame_size)
        this.outPtr = this.inputPtr + this.frame_size * FLOAT_SIZE
        this.outBuf = new Float32Array(this.wasm_df.memory.buffer, this.outPtr, this.frame_size)
        this.processOffset = 0
        this.readOffset = 0

        this.outRingBuf = new RingBuffer(3 * this.frame_ceil)
        this.prepareOutputFrame = new Float32Array(this.process_frame_size)
        //this.process_frame(new Float32Array(this.frame_size))
    }

    process_frame(input: Float32Array) { 
        const oldOffset = this.processOffset
        const inputTakeLength = Math.min(input.length, this.frame_ceil)
        const inputTake = input.slice(0, inputTakeLength)
        const moveOffset = Math.min(inputTakeLength, this.frame_size - oldOffset)
        //console.log(inputTake)

        this.inputBuf.set(input.slice(0, moveOffset), this.processOffset)
        if (this.processOffset + moveOffset >= this.frame_size) {
            this.processOffset = this.processOffset + moveOffset

            const inputLeftOver = inputTake.slice(moveOffset)
            this.processOffset = Math.min(inputLeftOver.length, this.frame_ceil - this.frame_size)

            this.wasm_df.df_process(this.model, this.inputPtr, this.frame_size, this.outPtr, this.frame_size)

            this.inputBuf.set(inputLeftOver.slice(0, this.processOffset))

            let i = 0;
            while (i < this.outBuf.length) {
                //console.log(this.outBuf.slice(i))
                const inserted = this.outRingBuf.write(this.outBuf.slice(i))
                i += inserted
            }
        } else {
            this.processOffset = this.processOffset + moveOffset
        }

        //let output = new Float32Array(this.process_frame_size)
        let i = 0;
        while (i < this.process_frame_size) {
            i += this.outRingBuf.read(this.prepareOutputFrame)
        }
        //console.log(this.prepareOutputFrame)

        return this.prepareOutputFrame
    }

    //process_frame(input: Float32Array) {
    //    const oldOffset = this.processOffset
    //    const inputTakeLength = Math.min(input.length, this.frame_ceil)
    //    const inputTake = input.slice(0, inputTakeLength)
    //    const moveOffset = Math.min(inputTakeLength, this.frame_size - oldOffset)
    //    //console.log(inputTake)
    //
    //    this.inputBuf.set(input.slice(0, moveOffset), this.processOffset)
    //    if (this.processOffset + moveOffset >= this.frame_size - 1) {
    //        this.processOffset = this.processOffset + moveOffset
    //
    //        const inputLeftOver = inputTake.slice(moveOffset)
    //        this.processOffset = Math.min(inputLeftOver.length, this.frame_ceil - this.frame_size)
    //
    //        console.assert(this.processOffset + this.frame_size === this.frame_ceil)
    //
    //        this.wasm_df.df_process(this.model, this.inputPtr, this.frame_size, this.outPtr + this.processOffset * FLOAT_SIZE, this.frame_size)
    //
    //        this.inputBuf.set(inputLeftOver.slice(0, this.processOffset))
    //    } else {
    //        this.processOffset = this.processOffset + moveOffset
    //    }
    //
    //    const startReadOffset = this.readOffset
    //    const endReadOffset = startReadOffset + this.process_frame_size
    //    this.readOffset += this.process_frame_size
    //
    //    if (this.readOffset >= this.frame_ceil) {
    //        //console.log(this.readOffset, startReadOffset, endReadOffset)
    //        this.readOffset = 0
    //    }
    //
    //    //let output = new Float32Array(this.process_frame_size)
    //    //output.set(this.outBuf.slice(startReadOffset, endReadOffset))
    //    //console.log(this.outBuf)
    //    //console.log(startReadOffset, endReadOffset)
    //    //return output
    //
    //    return this.outBuf.slice(startReadOffset, endReadOffset)
    //}

    set_atten(db: number) {
        this.wasm_df.set_atten_lim(this.model, db)
    }

    frame_length() {
        return this.frame_size
    }
}

class RingBuffer {
    buffer: Float32Array;
    capacity: number;
    writeHead: number;
    readHead: number;

    constructor(capacity: number) {
        if (capacity <= 0) {
            throw new Error("Capacity must be a positive number.");
        }
        this.capacity = capacity;
        this.buffer = new Float32Array(capacity);
        this.writeHead = 0;
        this.readHead = 0;
    }

    /**
     * Writes an array of Float32s into the buffer.
     * Returns the number of samples actually written.
     */
    write(input: Float32Array): number {
        const inputLength = input.length;
        const availableSpace = this.capacity - this.writeHead;
        const samplesToWrite = Math.min(inputLength, availableSpace);

        if (samplesToWrite === 0) {
            //console.log("empty buffer")
            return 0; // Buffer is full or input is empty
        }
        const takeInput = input.slice(0, samplesToWrite)

        this.buffer
            .set(takeInput, this.writeHead)

        this.writeHead = (this.writeHead + samplesToWrite) % this.capacity;
        //console.log(this.writeHead)

        return samplesToWrite;
    }

    /**
     * Reads an array of Float32s from the buffer into an output array.
     * Returns the number of samples actually read.
     */
    read(output: Float32Array): number {
        const outputLength = output.length;
        const sampleLeftToRead = this.capacity - this.readHead;
        const samplesToRead = Math.min(outputLength, sampleLeftToRead);

        if (samplesToRead === 0) {
            //console.log('empty read')
            //output.fill(0.0); // Fill output with silence if no data
            return 0; // Buffer is empty or output is empty
        }

        output
            .set(this.buffer.slice(this.readHead, this.readHead + samplesToRead))

        this.readHead = (this.readHead + samplesToRead) % this.capacity

        return samplesToRead;
    }

    // Helper to get how much data is currently in the buffer
    //get currentSize(): number {
    //    return this.size;
    //}
    //
    //// Helper to check if buffer is empty
    //get isEmpty(): boolean {
    //    return this.size === 0;
    //}
    //
    // Helper to check if buffer is full
    //get isFull(): boolean {
    //    return this.size === this.capacity;
    //}
    //
    // Clears the buffer (resets heads and size)
    clear() {
        this.buffer.fill(0.0);
        this.writeHead = 0;
        this.readHead = 0;
    }
}

//let ringBufTest = new RingBuffer(5)
//
//const input = new Float32Array([1, 2, 3, 4, 5, 6])
//
//let i = 0
//while (i < input.length) {
//    i += ringBufTest.write(input.slice(i))
//}
//let output = new Float32Array(3)
//
//ringBufTest.read(output)
//console.assert(output[0] === 6)
//console.assert(output[1] === 2)
//console.assert(output[2] === 3)




