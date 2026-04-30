/**
 * Cloudflare Worker - AI Image Analysis Proxy
 * 
 * Deploy this to Cloudflare Workers to securely call Google Gemini API
 * Your API key stays on the server, never exposed to clients.
 * 
 * SETUP:
 * 1. Create a Cloudflare account and go to Workers
 * 2. Create a new Worker and paste this code
 * 3. Add environment variable: GEMINI_API_KEY = your_api_key
 * 4. Deploy and copy the worker URL
 * 5. Update AI_CONFIG.endpoint in your HTML template
 */

export default {
    async fetch(request, env) {
        // Handle CORS preflight
        if (request.method === 'OPTIONS') {
            return new Response(null, {
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Max-Age': '86400'
                }
            });
        }

        if (request.method !== 'POST') {
            return new Response('Method not allowed', { status: 405 });
        }

        try {
            // Check API key exists
            if (!env.GEMINI_API_KEY) {
                console.error('GEMINI_API_KEY not found in environment');
                return new Response(JSON.stringify({ 
                    error: 'API key not configured',
                    details: 'GEMINI_API_KEY environment variable is missing'
                }), {
                    status: 500,
                    headers: { 
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }

            const body = await request.json();
            const { image, mimeType, prompt, fieldName, config } = body;

            console.log('Received request for field:', fieldName);
            console.log('Image size (chars):', image ? image.length : 0);
            console.log('Prompt length:', prompt ? prompt.length : 0);

            if (!image || !prompt) {
                return new Response(JSON.stringify({ error: 'Missing image or prompt' }), {
                    status: 400,
                    headers: { 
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                });
            }

            // Call Google Gemini API
            console.log('Calling Gemini API...');
            const geminiResponse = await callGemini(env.GEMINI_API_KEY, image, mimeType, prompt);
            console.log('Gemini response received:', JSON.stringify(geminiResponse).substring(0, 200));

            return new Response(JSON.stringify(geminiResponse), {
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            });

        } catch (error) {
            console.error('Worker error:', error.message);
            console.error('Error stack:', error.stack);
            return new Response(JSON.stringify({ 
                error: 'Analysis failed', 
                details: error.message 
            }), {
                status: 500,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            });
        }
    }
};

async function callGemini(apiKey, imageBase64, mimeType, prompt) {
    const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${apiKey}`;

    const requestBody = {
        contents: [{
            parts: [
                {
                    text: prompt
                },
                {
                    inline_data: {
                        mime_type: mimeType || 'image/jpeg',
                        data: imageBase64
                    }
                }
            ]
        }],
        generationConfig: {
            temperature: 0.2,
            topK: 32,
            topP: 1,
            maxOutputTokens: 2048,
            responseMimeType: "application/json"
        },
        safetySettings: [
            { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
            { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_NONE" }
        ]
    };

    const response = await fetch(GEMINI_URL, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Gemini API error: ${response.status} - ${errorText}`);
    }

    const data = await response.json();

    // Parse the response - Gemini returns structured JSON when we request it
    try {
        const textContent = data.candidates[0].content.parts[0].text;
        // Clean up any markdown code blocks if present
        const cleanJson = textContent.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        return JSON.parse(cleanJson);
    } catch (parseError) {
        // If parsing fails, return a structured error response
        return {
            score: 3,
            passed: false,
            description: data.candidates?.[0]?.content?.parts?.[0]?.text || 'Unable to parse AI response',
            elementsFound: [],
            elementsMissing: [],
            qualityIssues: ['AI response could not be parsed'],
            clearanceWarnings: [],
            feedback: 'Please manually review this image.',
            retakeRequired: false,
            rawResponse: data
        };
    }
}
