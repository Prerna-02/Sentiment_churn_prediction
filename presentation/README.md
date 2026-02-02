# Sentiment & Churn Prediction Dashboard - Presentation

## 📋 Overview
HTML-based presentation created with Reveal.js featuring a neon-teal glassmorphic theme.

## 🎨 Design Features
- **Neon-teal color scheme** with subtle glow effects
- **Sharp glassmorphic cards** with high contrast
- **White/light text** for maximum readability
- **Smooth slide transitions**
- **Responsive design**

## 📁 File Structure
```
presentation/
├── index.html              # Main presentation file
├── css/
│   └── custom.css          # Custom neon-teal theme
├── images/                 # Image assets (add your images here)
│   ├── university-logo.png       # SP Jain logo
│   └── dashboard-screenshot.png  # Your dashboard screenshot
└── README.md               # This file
```

## 🖼️ Required Images

You need to add these images to the `images/` folder:

1. **university-logo.png** - SP Jain School of Global Management logo
   - The logo you provided should be saved here
   - Recommended size: 400-600px width

2. **dashboard-screenshot.png** - Screenshot of your dashboard
   - Take a full-screen screenshot of your running dashboard
   - Recommended resolution: 1920x1080 or higher

## 🚀 How to Use

### Option 1: Open Directly in Browser
1. Simply open `index.html` in any modern web browser
2. Use arrow keys or controls to navigate

### Option 2: Serve with Local Server (Recommended)
```bash
# Navigate to presentation folder
cd presentation

# Using Python
python -m http.server 8080

# Using Node.js
npx http-server -p 8080

# Then open browser to http://localhost:8080
```

## ⌨️ Keyboard Controls

- **Arrow Keys**: Navigate between slides
- **Space**: Next slide
- **Shift + Space**: Previous slide
- **ESC**: Overview mode (see all slides)
- **F**: Fullscreen mode
- **S**: Speaker notes (if added)
- **Home**: First slide
- **End**: Last slide

## 📊 Slide Overview

1. **Title Slide** - University logo, your name, project details
2. **Introduction** - Problem statement and solution
3. **Objectives** - 5 key project objectives
4. **Literature Review** - Background research and technologies
5. **Architecture** - System architecture diagram and data flow
6. **Tech Stack** - Technologies used by layer
7. **Kafka & Spark** - Why these technologies were chosen
8. **Dashboard Overview** - Screenshot and key features
9. **MongoDB Analytics** - Database performance and queries
10. **Technical Achievements** - System performance metrics
11. **References** - Academic and technical sources

## 🎯 Presentation Tips

1. **Practice Navigation**: Familiarize yourself with slide transitions
2. **Test Images**: Ensure both images load correctly
3. **Check Readability**: Test on the actual projector/screen if possible
4. **Timing**: Aim for 10-15 minutes total presentation
5. **Key Points**: 
   - Emphasize real-time capabilities
   - Highlight MongoDB aggregation pipelines
   - Explain why Kafka & Spark are industry standards
   - Show the dashboard screenshot prominently

## 📤 Export to PDF (Optional)

To create a PDF version for submission:

1. Open the presentation in Chrome/Chromium
2. Add `?print-pdf` to the URL: `index.html?print-pdf`
3. Press `Ctrl+P` (Windows) or `Cmd+P` (Mac)
4. Select "Save as PDF"
5. Ensure "Background graphics" is enabled

## 🎨 Customization

### Changing Colors
Edit `css/custom.css` and modify the `:root` variables:
```css
:root {
    --neon-teal: #00ffff;      /* Primary color */
    --neon-accent: #00ff88;    /* Accent color */
    --bg-dark: #0a0a1a;        /* Background */
}
```

### Adjusting Glow Intensity
Search for `text-shadow` and `box-shadow` in `custom.css` and adjust the alpha values (last number in rgba).

## 🔧 Troubleshooting

**Images not showing:**
- Check that image files exist in `images/` folder
- Verify file names match exactly (case-sensitive)
- Use relative paths, not absolute paths

**Text hard to read:**
- Increase contrast in `custom.css`
- Adjust `--text-white` and `--text-light` variables
- Test on actual presentation display

**Layout issues:**
- Presentation is optimized for 1280x720 (16:9)
- Browser zoom should be at 100%
- Use fullscreen mode (F key)

## 📝 Subject Code Update

If your subject code is different from "Introduction to Databases", update it in `index.html`:

```html
<p class="subject">Introduction to Databases (YOUR_CODE_HERE)</p>
```

## ✨ Final Checklist

Before presenting:
- [ ] University logo added to `images/` folder
- [ ] Dashboard screenshot added to `images/` folder
- [ ] Test presentation in browser
- [ ] Verify all slides display correctly
- [ ] Check text readability on projector
- [ ] Practice keyboard navigation
- [ ] Test on presentation laptop/computer
- [ ] Have backup (PDF export or USB drive)

## 📧 Questions?

This presentation was custom-built for your project. The neon-teal theme with subtle glows ensures professional appearance while maintaining high readability.

Good luck with your presentation! 🚀
