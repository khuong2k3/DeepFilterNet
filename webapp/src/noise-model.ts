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
    inRingBuf: RingBuffer
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
        //console.log('model: ', modelBytesCopy, modelPtr)

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

        //this.inRingBuf = new RingBuffer(2 * this.frame_ceil)
        this.outRingBuf = new RingBuffer(2 * this.frame_size)
        this.outRingBuf.write(new Float32Array(this.frame_ceil))
        this.prepareOutputFrame = new Float32Array(this.process_frame_size)
        //this.process_frame(new Float32Array(this.frame_size))
    }

    process_frame(input: Float32Array) {
        const inputTakeLength = this.process_frame_size
        const inputTake = input.subarray(0, inputTakeLength)
        const emptyFrame = this.frame_size - this.processOffset
        const moveOffset = Math.min(inputTakeLength, emptyFrame)

        this.inputBuf.set(input.subarray(0, moveOffset), this.processOffset)
        this.processOffset += inputTakeLength
        if (this.processOffset >= this.frame_size) {
            this.wasm_df.df_process(this.model, this.inputPtr, this.frame_size, this.outPtr, this.frame_size)

            this.processOffset -= this.frame_size
            const inputLeftOver = inputTake.subarray(moveOffset, inputTakeLength)
            console.assert(this.processOffset === inputLeftOver.length)

            if (inputLeftOver.length > 0) {
                this.inputBuf.set(inputLeftOver)
                console.assert(this.inputBuf[0] === inputLeftOver[0])
            }

            let i = 0;
            while (i < this.outBuf.length) {
                i += this.outRingBuf.write(this.outBuf, i)
            }
            console.assert(i === this.outBuf.length)
        }

        let i = 0;
        while (i < this.process_frame_size) {
            i += this.outRingBuf.read(this.prepareOutputFrame.subarray(i))
        }
        //console.assert(i === this.process_frame_size)
        //for (let i = 0; i < this.process_frame_size; i++) {
        //    console.assert(this.prepareOutputFrame[i] === this.outBuf[i])
        //}
        //
        //this.prepareOutputFrame.set(this.outBuf)
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


/**
 * Implements a circular buffer (ring buffer) for Float32Array data.
 * This is highly optimized for audio processing to efficiently manage
 * continuous streams of fixed-size blocks of data.
 */
class RingBuffer {
    buffer: Float32Array; // The underlying Float32Array that stores the data
    capacity: number;     // The maximum number of samples the buffer can hold
    writeHead: number;    // The index where the next incoming sample will be written
    readHead: number;     // The index from where the next outgoing sample will be read
    size: number;         // The current number of samples actually stored in the buffer

    /**
     * Creates a new RingBuffer instance.
     * @param capacity The maximum number of Float32 samples the buffer can hold. Must be a positive integer.
     */
    constructor(capacity: number) {
        if (!Number.isInteger(capacity) || capacity <= 0) {
            throw new Error("RingBuffer capacity must be a positive integer.");
        }
        this.capacity = capacity;
        this.buffer = new Float32Array(capacity);
        this.buffer.fill(0); // Initialize buffer with zeros to ensure silence on empty reads
        this.writeHead = 0;
        this.readHead = 0;
        this.size = 0; // The buffer starts empty
    }

    /**
     * Writes a Float32Array of samples into the buffer.
     * Data is copied from `input` starting at `inputOffset`.
     * If the buffer does not have enough space, it writes only the available space.
     *
     * @param input The Float32Array containing the source data to write.
     * @param inputOffset The optional offset within the `input` array to start reading from. Defaults to 0.
     * @returns The number of samples actually written to the buffer.
     */
    write(input: Float32Array, inputOffset: number = 0): number {
        // Calculate the actual length of data we intend to copy from the input array
        const dataLengthToCopy = input.length - inputOffset;
        // Calculate how much space is truly available in the ring buffer
        const availableSpace = this.capacity - this.size;
        // Determine how many samples we can actually write (min of input data and available space)
        const samplesToWrite = Math.min(dataLengthToCopy, availableSpace);

        if (samplesToWrite <= 0) {
            return 0; // No space in buffer or no valid input data
        }

        // Create a subarray from the input to efficiently get the portion we need.
        // This avoids unnecessary allocations and ensures correct offset.
        const inputSource = input.subarray(inputOffset, inputOffset + samplesToWrite);

        // Calculate how much can be written from the current `writeHead` to the end of the buffer.
        // This is the first contiguous chunk before a potential wrap-around.
        const firstChunkLength = Math.min(samplesToWrite, this.capacity - this.writeHead);

        // Copy the first chunk directly into the buffer.
        this.buffer.set(inputSource.subarray(0, firstChunkLength), this.writeHead);

        // If there's a second chunk, it means we wrapped around the buffer's end.
        const secondChunkLength = samplesToWrite - firstChunkLength;
        if (secondChunkLength > 0) {
            // Copy the remaining data to the beginning of the buffer.
            this.buffer.set(inputSource.subarray(firstChunkLength), 0);
        }

        // Update the write head and the current size of the buffer.
        this.writeHead = (this.writeHead + samplesToWrite) % this.capacity;
        this.size += samplesToWrite;

        return samplesToWrite;
    }

    /**
     * Reads a specified number of Float32 samples from the buffer into an output array.
     * Data is copied into `output` starting at `outputOffset`.
     * If not enough data is available in the buffer, it reads all available data
     * and fills the remaining portion of the `output` array (or its relevant part) with silence (zeros).
     *
     * @param output The Float32Array to write the read data into.
     * @param outputOffset The optional offset within the `output` array to start writing to. Defaults to 0.
     * @returns The number of samples actually read from the buffer.
     */
    read(output: Float32Array, outputOffset: number = 0): number {
        // Calculate the actual space available in the output array for reading into.
        const outputSpace = output.length - outputOffset;
        // Determine how much data is truly available in the ring buffer.
        const availableData = this.size;
        // Determine how many samples we can actually read (min of output space and available data).
        const samplesToRead = Math.min(outputSpace, availableData);

        // Get a subarray view of the output buffer where data will be placed.
        const outputTarget = output.subarray(outputOffset, outputOffset + outputSpace);

        if (samplesToRead <= 0) {
            // If no data to read, fill the entire target output portion with silence.
            outputTarget.fill(0.0);
            return 0;
        }

        // Calculate how much can be read from the current `readHead` to the end of the buffer.
        // This is the first contiguous chunk before a potential wrap-around.
        const firstChunkLength = Math.min(samplesToRead, this.capacity - this.readHead);

        // Copy the first chunk from the buffer to the output target.
        outputTarget.set(this.buffer.subarray(this.readHead, this.readHead + firstChunkLength), 0);

        // If there's a second chunk, it means we wrapped around the buffer's end.
        const secondChunkLength = samplesToRead - firstChunkLength;
        if (secondChunkLength > 0) {
            // Copy the remaining data from the beginning of the buffer to the output target.
            outputTarget.set(this.buffer.subarray(0, secondChunkLength), firstChunkLength);
        }

        // Update the read head and the current size of the buffer.
        this.readHead = (this.readHead + samplesToRead) % this.capacity;
        this.size -= samplesToRead;

        // If less data was read than the `outputSpace` requested (e.g., due to an underrun),
        // fill the remaining portion of the `output` array (starting from `samplesToRead`) with silence.
        if (samplesToRead < outputSpace) {
            outputTarget.fill(0.0, samplesToRead); // Fill from `samplesToRead` to the end of `outputTarget`
        }

        return samplesToRead;
    }

    /**
     * Returns the current number of elements (samples) stored in the buffer.
     */
    get currentSize(): number {
        return this.size;
    }

    /**
     * Checks if the buffer is completely empty.
     */
    get isEmpty(): boolean {
        return this.size === 0;
    }

    /**
     * Checks if the buffer is completely full.
     */
    get isFull(): boolean {
        return this.size === this.capacity;
    }

    /**
     * Clears the buffer, resetting write/read heads and size to zero,
     * and filling the underlying buffer with zeros (silence).
     */
    clear() {
        this.buffer.fill(0.0); // Reset all values to silence
        this.writeHead = 0;
        this.readHead = 0;
        this.size = 0;
    }
}



//class RingBuffer {
//    buffer: Float32Array;
//    capacity: number;
//    writeHead: number;
//    readHead: number;
//    size: number;
//
//    constructor(capacity: number) {
//        if (capacity <= 0) {
//            throw new Error("Capacity must be a positive number.");
//        }
//        this.capacity = capacity;
//        this.buffer = new Float32Array(capacity);
//        this.writeHead = 0;
//        this.readHead = 0;
//        this.size = 0;
//    }
//
//    /**
//     * Writes an array of Float32s into the buffer.
//     * Returns the number of samples actually written.
//     */
//    write(input: Float32Array, bufOffset: number = 0): number {
//        const inputLength = input.length;
//        const availableSpace = this.capacity - this.writeHead;
//        const samplesToWrite = Math.min(inputLength, availableSpace);
//
//        if (samplesToWrite === 0 || bufOffset === inputLength) {
//            //console.log("empty buffer")
//            return 0; // Buffer is full or input is empty
//        }
//
//
//        const takeInput = input.subarray(bufOffset, bufOffset + samplesToWrite)
//
//        this.buffer
//            .set(takeInput, this.writeHead)
//
//        this.writeHead += this.writeHead
//        this.size += samplesToWrite
//        if (this.writeHead >= this.capacity) {
//            this.writeHead -= this.capacity;
//            //(this.writeHead + samplesToWrite) % this.capacity;
//        }
//        //console.log(this.writeHead)
//
//        return samplesToWrite;
//    }
//
//    /**
//     * Reads an array of Float32s from the buffer into an output array.
//     * Returns the number of samples actually read.
//     */
//    read(output: Float32Array, bufOffset: number = 0): number {
//        const outputLength = output.length;
//        const sampleLeftToRead = Math.min(this.size, this.capacity - this.readHead);
//        const samplesToRead = Math.min(outputLength, sampleLeftToRead);
//
//        if (samplesToRead === 0 || bufOffset === outputLength) {
//            //console.log('empty read')
//            //output.fill(0.0); // Fill output with silence if no data
//            return 0; // Buffer is empty or output is empty
//        }
//
//        const readBuffer = this.buffer.subarray(this.readHead, this.readHead + samplesToRead)
//        output
//            .set(readBuffer, bufOffset)
//
//        this.readHead += samplesToRead
//        this.size -= samplesToRead
//        if (this.readHead >= this.capacity) {
//            //console.log(this.readHead)
//            this.readHead -= this.capacity
//        }
//
//        return samplesToRead;
//    }
//
//    // Helper to get how much data is currently in the buffer
//    //get currentSize(): number {
//    //    return this.size;
//    //}
//    //
//    //// Helper to check if buffer is empty
//    //get isEmpty(): boolean {
//    //    return this.size === 0;
//    //}
//    //
//    // Helper to check if buffer is full
//    //get isFull(): boolean {
//    //    return this.size === this.capacity;
//    //}
//    //
//    // Clears the buffer (resets heads and size)
//    clear() {
//        this.buffer.fill(0.0);
//        this.writeHead = 0;
//        this.readHead = 0;
//    }
//}

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




