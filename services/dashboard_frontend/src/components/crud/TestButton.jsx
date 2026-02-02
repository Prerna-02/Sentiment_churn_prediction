// src/components/crud/TestButton.jsx
import { useState } from 'react';

function TestButton() {
    const [count, setCount] = useState(0);

    const handleClick = () => {
        console.log('Button clicked!', count);
        alert(`Button clicked! Count: ${count}`);
        setCount(count + 1);
    };

    return (
        <div className="glass-card p-8">
            <h2 className="text-2xl font-bold text-white mb-4">Button Test</h2>
            <p className="text-slate-300 mb-4">Click count: {count}</p>
            <button
                onClick={handleClick}
                className="px-6 py-3 bg-blue-500 text-white rounded-lg hover:bg-blue-600"
            >
                Click Me to Test
            </button>
        </div>
    );
}

export default TestButton;
