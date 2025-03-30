import React, { useState, useEffect, useRef } from "react";
import { Button, HStack, VStack, Text, Box, Textarea, Image, Grid} from "@chakra-ui/react";

const actions = [
  "Data Collection",
  "Load Data",
  "Data Processing",
  "Run LSTM Model",
  "Run VAR Model",
  "Run ARIMA Model",
  "Inference"
];

const plotImages = [
  { name: "LSTM", file: "LSTM_evaluation_plot.png" },
  { name: "VAR", file: "VAR_evaluation_plot.png" },
  { name: "ARIMA", file: "ARIMA_evaluation_plot.png" }
];

const App = () => {
  const [response, setResponse] = useState("");
  const [logs, setLogs] = useState("");
  const logsRef = useRef(null);
  const [imageUpdated, setImageUpdated] = useState(false);
  const [showImage, setShowImage] = useState(false);

  useEffect(() => {
    if (logsRef.current) {
      logsRef.current.scrollTop = logsRef.current.scrollHeight;
    }
  }, [logs]);

  const handleClick = async (action) => {
    setResponse(`Starting ${action}...`);
    setLogs("");

    try {
      const res = await fetch("http://127.0.0.1:8000/process", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ action }),
      });

      if (!res.ok) {
        throw new Error("Failed to start process");
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder("utf-8");

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        setLogs((prevLogs) => prevLogs + decoder.decode(value));
      }

      setResponse(`${action} completed successfully.`);

      // Only show images after inference
      if (action === "Inference") {
        setImageUpdated((prev) => !prev);
        setShowImage(true);
      }
    } catch (error) {
      setResponse(`Error: ${error.message}`);
      console.error(error);
    }
  };

  return (
    <VStack spacing={6} p={5} align="center" bg="black" minH="100vh" color="white">
      <VStack spacing={4} width="100%">
        {/* Data Pipeline Buttons */}
        <HStack spacing={4} wrap="wrap" justify="center">
          {actions.slice(0, 3).map((action) => (
            <Button
              key={action}
              bg="black"
              color="white"
              border="2px solid #00bfff"
              _hover={{ bg: "#00bfff", color: "black" }}
              onClick={() => handleClick(action)}
            >
              {action}
            </Button>
          ))}
        </HStack>

        <Box borderBottom="2px solid #00bfff" width="100%" my={4} />

        {/* Model Buttons */}
        <HStack spacing={4} wrap="wrap" justify="center">
          {actions.slice(3, 6).map((action) => (
            <Button
              key={action}
              bg="black"
              color="white"
              border="2px solid #00bfff"
              _hover={{ bg: "#00bfff", color: "black" }}
              onClick={() => handleClick(action)}
            >
              {action}
            </Button>
          ))}
        </HStack>

        <Box borderBottom="2px solid #00bfff" width="100%" my={4} />

        {/* Inference Button */}
        <Button
          bg="black"
          color="white"
          border="2px solid #00bfff"
          _hover={{ bg: "#00bfff", color: "black" }}
          onClick={() => handleClick(actions[6])}
        >
          {actions[6]}
        </Button>
      </VStack>

      {response && (
        <Text fontSize="lg" fontWeight="bold" color="#00bfff">
          {response}
        </Text>
      )}

      {showImage && (
        <Grid templateColumns="repeat(1, 1fr)" gap={6} width="100%" maxW="1200px">
          {plotImages.map((plot) => (
            <Box
              key={plot.name}
              borderRadius="md"
              overflow="hidden"
              border="2px solid #00bfff"
              boxShadow="0 0 15px #00bfff"
            >
              <Text fontSize="lg" fontWeight="bold" color="#00bfff" p={2} textAlign="center">
                {plot.name} Model Results
              </Text>
              <Image
                key={imageUpdated}
                src={`http://127.0.0.1:8000/static/${plot.file}?timestamp=${new Date().getTime()}`}
                alt={`${plot.name} Model Result`}
                maxWidth="100%"
                fallbackSrc="https://via.placeholder.com/600x400?text=No+Image"
              />
            </Box>
          ))}
        </Grid>
      )}

      <Box width="100%" maxW="600px">
        <Text fontSize="lg" fontWeight="bold" mb={2} color="#00bfff">
          Live Logs:
        </Text>
        <Textarea
          ref={logsRef}
          value={logs}
          readOnly
          height="300px"
          fontSize="sm"
          whiteSpace="pre-wrap"
          overflowY="scroll"
          bg="black"
          color="white"
          border="2px solid #00bfff"
          _focus={{ borderColor: "#00bfff", boxShadow: "0 0 10px #00bfff" }}
        />
      </Box>
    </VStack>
  );
};

export default App;
