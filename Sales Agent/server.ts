import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { GoogleGenerativeAI, Part } from "@google/generative-ai";
import * as dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// INITIALIZE GEMINI
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');
const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

app.use(cors());
app.use(express.json());

// Multi-part form data for image uploads
const upload = multer({ storage: multer.memoryStorage() });

app.post('/analyze-image', upload.single('image'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No image file provided' });
        }

        const rubric = req.body.rubric || "General solar inspection quality check.";

        // Convert buffer to generative AI part
        const imagePart: Part = {
            inlineData: {
                data: req.file.buffer.toString('base64'),
                mimeType: req.file.mimetype
            }
        };

        const prompt = `
            Evaluate this photo for a solar site inspection.
            
            RUBRIC: ${rubric}

            You must return a JSON object with strictly these keys:
            - "score": (number 1-5) Quality and compliance score.
            - "passed": (boolean) true if score >= 4.
            - "description": (string) Tell us exactly what you see in the photo in natural language.
            - "itemsFound": (string array) List the technical items/components identified.
            - "inverterDetected": (boolean) Explicitly state if an inverter is visible.
            - "feedback": (string) Recommendation for the user.

            Return ONLY the raw JSON object.
        `;

        const result = await model.generateContent([prompt, imagePart]);
        const response = await result.response;
        const text = response.text();

        // Clean up text if LLM wrapped it in markdown code blocks
        const jsonMatch = text.match(/\{[\s\S]*\}/);
        const jsonResponse = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);

        res.json(jsonResponse);
    } catch (error) {
        console.error('AI Processing Error:', error);
        res.status(500).json({ error: 'Failed to analyze image' });
    }
});

app.listen(port, () => {
    console.log(`AI Proxy running at http://localhost:${port}`);
});
