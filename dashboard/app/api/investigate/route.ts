import { NextRequest, NextResponse } from 'next/server';

const API_URL = process.env.INVESTIGATION_API_URL || 'http://localhost:8000';

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();

        const response = await fetch(`${API_URL}/api/v1/investigate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(45000), // 45s timeout
        });

        if (!response.ok) {
            const error = await response.text();
            return NextResponse.json(
                { error: `Investigation failed: ${error}` },
                { status: response.status }
            );
        }

        const data = await response.json();
        return NextResponse.json(data);
    } catch (error: any) {
        if (error.name === 'TimeoutError') {
            return NextResponse.json(
                { error: 'Investigation timed out. The domain may be unreachable.' },
                { status: 504 }
            );
        }
        return NextResponse.json(
            { error: `Failed to connect to investigation service: ${error.message}` },
            { status: 502 }
        );
    }
}
