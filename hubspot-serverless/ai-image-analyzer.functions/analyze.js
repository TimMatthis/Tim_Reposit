/**
 * HubSpot Serverless Function - AI Image Analyzer
 * 
 * Proxies requests to Google Gemini API for image analysis
 * Keeps API key secure on server-side
 */

const https = require('https');

exports.main = async (context, sendResponse) => {
    // Set CORS headers
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json'
    };

    // Handle preflight
    if (context.method === 'OPTIONS') {
        return sendResponse({ statusCode: 200, headers, body: '' });
    }

    try {
        const { image, mimeType, prompt, fieldName } = context.body;

        if (!image || !prompt) {
            return sendResponse({
                statusCode: 400,
                headers,
                body: JSON.stringify({ error: 'Missing image or prompt' })
            });
        }

        const apiKey = process.env.GEMINI_API_KEY;
        
        if (!apiKey || apiKey === 'YOUR_GEMINI_API_KEY_HERE') {
            return sendResponse({
                statusCode: 500,
                headers,
                body: JSON.stringify({ error: 'API key not configured' })
            });
        }

        // Call Gemini API
        const geminiResult = await callGeminiAPI(apiKey, image, mimeType, prompt);

        return sendResponse({
            statusCode: 200,
            headers,
            body: JSON.stringify(geminiResult)
        });

    } catch (error) {
        console.error('Function error:', error);
        return sendResponse({
            statusCode: 500,
            headers,
            body: JSON.stringify({ 
                error: 'Analysis failed', 
                details: error.message 
            })
        });
    }
};

function callGeminiAPI(apiKey, imageBase64, mimeType, prompt) {
    return new Promise((resolve, reject) => {
        const requestBody = JSON.stringify({
            contents: [{
                parts: [
                    { text: prompt },
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
        });

        const options = {
            hostname: 'generativelanguage.googleapis.com',
            path: `/v1beta/models/gemini-1.5-flash:generateContent?key=${apiKey}`,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(requestBody)
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            
            res.on('data', chunk => { data += chunk; });
            
            res.on('end', () => {
                try {
                    const response = JSON.parse(data);
                    
                    if (res.statusCode !== 200) {
                        reject(new Error(`Gemini API error: ${res.statusCode}`));
                        return;
                    }

                    // Parse the AI response
                    const textContent = response.candidates?.[0]?.content?.parts?.[0]?.text;
                    
                    if (!textContent) {
                        resolve({
                            score: 3,
                            passed: false,
                            description: 'AI returned empty response',
                            feedback: 'Please try again or manually review.',
                            retakeRequired: false
                        });
                        return;
                    }

                    // Clean and parse JSON response
                    const cleanJson = textContent
                        .replace(/```json\n?/g, '')
                        .replace(/```\n?/g, '')
                        .trim();
                    
                    const result = JSON.parse(cleanJson);
                    resolve(result);

                } catch (parseError) {
                    console.error('Parse error:', parseError);
                    resolve({
                        score: 3,
                        passed: false,
                        description: 'Could not parse AI response',
                        elementsFound: [],
                        elementsMissing: [],
                        qualityIssues: ['Response parsing failed'],
                        clearanceWarnings: [],
                        feedback: 'Please manually review this image.',
                        retakeRequired: false
                    });
                }
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.write(requestBody);
        req.end();
    });
}
