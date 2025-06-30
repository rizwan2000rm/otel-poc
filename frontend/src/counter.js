import { tracer } from "./otel-setup.js";

export function setupCounter(element) {
  let counter = 0;
  const setCounter = (count) => {
    counter = count;
    element.innerHTML = `count is ${counter}`;
  };
  element.addEventListener("click", () => {
    const span = tracer.startSpan("button.click");
    setCounter(counter + 1);
    span.end();
  });
  setCounter(0);
}
