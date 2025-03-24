import React, { useState, useEffect, useRef } from "react";
import { Button, HStack, VStack, Text, Box, Textarea, Image } from "@chakra-ui/react";

const actions = ["Data Collection", "Load Data", "Data Processing", "Modelling", "Inference"];

const App = () => {
  const [response, setResponse] = useState("");
  const [logs, setLogs] = useState("");
  const logsRef = useRef(null);
  const [imageUpdated, setImageUpdated] = useState(false);
  const [showImage, setShowImage] = useState(false); // Initially hidden

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

      // Only show image after inference
      if (action === "Inference") {
        setImageUpdated((prev) => !prev); // Force image refresh
        setShowImage(true); // Show the image
      }
    } catch (error) {
      setResponse(`Error: ${error.message}`);
      console.error(error);
    }
  };

  return (
    <VStack spacing={6} p={5} align="center" bg="black" minH="100vh" color="white">
      <HStack spacing={4}>
        {actions.map((action) => (
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

      {response && (
        <Text fontSize="lg" fontWeight="bold" color="#00bfff">
          {response}
        </Text>
      )}

      {/* Show image only after inference completes */}
      {showImage && (
        <Box maxW="600px" borderRadius="md" overflow="hidden" border="2px solid #00bfff" boxShadow="0 0 15px #00bfff">
          <Image
            key={imageUpdated} // Forces re-render when image updates
            src={`http://127.0.0.1:8000/static/stock_plot.png?timestamp=${new Date().getTime()}`}
            alt="Inference Result"
            maxWidth="100%"
            fallbackSrc="https://via.placeholder.com/600x400?text=No+Image"
          />
        </Box>
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
