import { createEffect, createSignal } from 'solid-js'
import './App.css'
import { CMAP_INFERNO } from './cmap';

async function fetchModel() {
  const response = await fetch("/DeepFilterNet3_onnx.tar.gz");
  const arrayBuffer = await response.arrayBuffer()
  return arrayBuffer
}

async function setupAudioWorklet(audioCtx: AudioContext) {
  const worketURL = new URL('worklet.js', import.meta.url)
  const wasmURL = new URL('./pkg/df_audio_worklet_bg.wasm', import.meta.url)
  const modelArrayTar = await fetchModel()
  const modelBytes = new Uint8Array(modelArrayTar)
  console.log("model bytes: ", modelBytes)
  const wasmRes = await fetch(wasmURL)
  const wasmBuffer = await wasmRes.arrayBuffer()
  const wasmBytes = new Uint8Array(wasmBuffer)

  await audioCtx.audioWorklet.addModule(worketURL)

  const audioNode = new AudioWorkletNode(audioCtx, 'WasmProcessor', {
    processorOptions: {
      //modelBytes
      wasmBytes, modelBytes
    }
  })

  return audioNode
}

function Loading() {
  const [dotNum, setDotNum] = createSignal<number>(0)

  setInterval(() => {
    setDotNum(dotNum() % 3 + 1)
  }, 400)

  return <>
    {
      "Loading" + ".".repeat(dotNum())
    }
  </>
}

function App() {
  const [file, setFile] = createSignal<File | null>(null);
  const [downloadUrl, setDownloadUrl] = createSignal<string>('');
  const [disableInput, setDisableInput] = createSignal<boolean>(false);
  const [audioNode, setAudioNode] = createSignal<AudioWorkletNode>(null);
  const audioCtx = new AudioContext()

  const [loading, setLoading] = createSignal<boolean>(false);

  setupAudioWorklet(audioCtx).then((node) => {
    setAudioNode(node)
  })

  createEffect(() => {
    if (file() !== null && audioNode() !== null) {
      const reader = new FileReader();
      reader.addEventListener("load", async (event) => {
        setLoading(true)
        const audioArray = event.target.result as ArrayBuffer;
        const audioBuffer = await audioCtx.decodeAudioData(audioArray)

        console.log(audioNode())
        audioNode().connect(audioCtx.destination)

        visualize(audioBuffer, audioCtx)

        setLoading(false)
      })
      reader.readAsArrayBuffer(file())
    }
  });

  return (
    <>
      <div class="outer-box">
        <input
          type='file'
          disabled={disableInput()}
          onChange={(e) => {
            if (e.target.files !== null) {
              setFile(e.target.files[0])
            }
          }} />
        <div>
          {loading() && <Loading />}
        </div>
        {
          downloadUrl() !== '' &&
          <a href={downloadUrl()} download={file().name}>Download</a>
        }

        <canvas id="audio-canvas" />
      </div>
    </>
  )
}

function visualize(audioBuffer: AudioBuffer, audioCtx: AudioContext) {
  const canvas = document.getElementById('audio-canvas') as HTMLCanvasElement

  canvas.width = 300
  canvas.height = 200
  const source = audioCtx.createBufferSource()
  source.buffer = audioBuffer // register audio source

  const analyzer = audioCtx.createAnalyser()
  analyzer.fftSize = 256
  source.connect(analyzer)
  analyzer.connect(audioCtx.destination)
  source.start()

  console.log(analyzer)
  const specVisualyzer = new SpecVisualizer(canvas, 100, analyzer)

  setInterval(() => {
    specVisualyzer.update()
  }, 50)
}

class SpecVisualizer {
  canvas: HTMLCanvasElement
  canvasCtx: CanvasRenderingContext2D
  barHeight: number
  barWidth: number
  window_size: number
  analyser: AnalyserNode
  specs: RingBuf<Uint8Array>
  frequencyData: Uint8Array

  constructor(canvas: HTMLCanvasElement, window_size: number, analyser: AnalyserNode) {
    this.canvas = canvas
    this.canvasCtx = canvas.getContext('2d')
    this.window_size = window_size
    this.barHeight = this.canvas.height / analyser.frequencyBinCount
    this.barWidth = this.canvas.width / window_size
    this.frequencyData = new Uint8Array(analyser.frequencyBinCount)
    this.analyser = analyser

    this.specs = new RingBuf(window_size, () => new Uint8Array(analyser.frequencyBinCount))
  }

  update() {
    this.analyser.getByteFrequencyData(this.frequencyData)
    this.specs.push_update((array) => { array.set(this.frequencyData) })

    let specView = new Array<Uint8Array>(this.window_size)
    this.specs.view(specView, 0)

    this.canvasCtx.clearRect(0, 0, this.canvas.width, this.canvas.height)
    for (let i = 0; i < specView.length - 1; i++) {
      let freqData = specView[i]
      for (let j = 0; j < freqData.length; j++) {
        this.canvasCtx.fillStyle = CMAP_INFERNO[freqData[j]]
        this.canvasCtx.fillRect(
          i * this.barWidth,
          (freqData.length - j - 1) * this.barHeight,
          this.barWidth,
          this.barHeight
        )
      }
    }
  }

}

class RingBuf<T> {
  bufInner: Array<T>
  start: number
  index: number
  constructor(length: number, defaultValue: () => T) {
    this.bufInner = Array.from({ length }, defaultValue)
    //(length, default)
    this.index = 0
    this.start = 0
  }

  push_update(fn: (value: T) => void) {
    fn(this.bufInner[this.index])
    this.index++
    if (this.index >= this.bufInner.length) {
      this.index = 0
      this.start++
    } else if (this.start >= this.index) {
      this.start++
    }

    if (this.start === this.bufInner.length) {
      this.start = 0
    }
  }

  push(value: T) {
    this.bufInner[this.index] = value
    this.index++
    if (this.index >= this.bufInner.length) {
      this.index = 0
      this.start++
    } else if (this.start >= this.index) {
      this.start++
    }

    if (this.start === this.bufInner.length) {
      this.start = 0
    }
  }

  view(buf: Array<T>, startIdx: number = 0) {
    let i = 0;
    const bufLength = buf.length
    let start = (this.start + startIdx) % bufLength

    while (i < bufLength) {
      const popOut = Math.min(this.bufInner.length - start, bufLength - i)
      buf.splice(i, popOut, ...this.bufInner.slice(start, start + popOut))
      i += popOut
      start += popOut

      if (start >= this.bufInner.length) {
        start = start - this.bufInner.length
      }
    }

    console.assert(bufLength === buf.length)
  }

  len() {
    return this.bufInner.length
  }

}

export default App



