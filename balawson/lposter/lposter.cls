% \iffalse meta-comment
% lposter.cls -- LaTeX class to create posters
% Based on a class by Claire Connelly <cmc@math.hmc.edu>
% \fi
%%
%% Poster LaTeX class file.
%%

\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{lposter}[]

%%% Declare and set some options.

\DeclareOption*{\PassOptionsToClass{\CurrentOption}{article}}
%\ExecuteOptions{}
\ProcessOptions\relax
\LoadClass{article}

\setlength{\paperwidth}{40in}
\setlength{\paperheight}{30in}
\setlength{\textwidth}{37in}
\setlength{\textheight}{22in}
\setlength{\topmargin}{0.5in}
\setlength{\oddsidemargin}{0.5in}
\setlength{\evensidemargin}{\oddsidemargin}
\setlength{\topmargin}{0.5in}
\setlength{\headheight}{4in} % 4 before
\setlength{\headsep}{0.5in}
\setlength{\footskip}{1in}
\setlength{\voffset}{0pt}
\setlength{\hoffset}{0pt}

%%% Define a command to include a logo.
\DeclareRobustCommand{\@logo}{%
  \begin{minipage}{9.5in}
    \includegraphics{BUCS}
  \end{minipage}
}

\RequirePackage{ifthen}
\RequirePackage{mathpazo}
\RequirePackage[scaled=0.95]{helvet}
\RequirePackage{sectsty}
\RequirePackage[font=small,font=sf,labelfont=bf]{caption}[]
\RequirePackage{graphics}
\RequirePackage{color}
\definecolor{ltgray}{rgb}{0.704,0.704,0.704}
\definecolor{black}{rgb}{0,0,0}
\definecolor{dkblue}{rgb}{0,0,0.47}
\definecolor{green}{rgb}{0,1,0}
\RequirePackage{multicol}
\setlength{\columnsep}{1in}
\setlength{\columnseprule}{2pt}

%% Redefine the named font sizes so we can keep most of the code
%%% intact whilst still having larger type.

% \renewcommand\LARGE{\fontsize{64pt}{72pt}\selectfont}
% \renewcommand\Large{\fontsize{48pt}{60pt}\selectfont}
% \renewcommand\large{\fontsize{36pt}{44pt}\selectfont}
% \renewcommand\normalsize{\fontsize{24pt}{30pt}\selectfont}

%\renewcommand{\LARGE}{\fontsize{72pt}{86pt}\selectfont}
%\renewcommand{\Large}{\fontsize{64pt}{72pt}\selectfont}
%\renewcommand{\large}{\fontsize{48pt}{60pt}\selectfont}
%\renewcommand{\normalsize}{\fontsize{36pt}{44pt}\selectfont}
%\renewcommand{\small}{\fontsize{30pt}{36pt}\selectfont}
%\renewcommand{\footnotesize}{\fontsize{24pt}{30pt}\selectfont}

\renewcommand{\LARGE}{\fontsize{64pt}{72pt}\selectfont}
\renewcommand{\Large}{\fontsize{48pt}{60pt}\selectfont}
\renewcommand{\large}{\fontsize{36pt}{44pt}\selectfont}
\renewcommand{\normalsize}{\fontsize{30pt}{36pt}\selectfont}
\renewcommand{\small}{\fontsize{24pt}{30pt}\selectfont}
\renewcommand{\footnotesize}{\fontsize{18pt}{24pt}\selectfont}

%\renewcommand\tiny{\fontsize{12}{14.4}\selectfont}
%\renewcommand\scriptsize{\fontsize{16.8}{19.2}\selectfont}
%\renewcommand\footnotesize{\fontsize{19.2}{22.8}\selectfont}
%\renewcommand\small{\fontsize{21.6}{26.4}\selectfont}
%\renewcommand\normalsize{\fontsize{24}{28.8}\selectfont}
%\renewcommand\large{\fontsize{28.8}{33.6}\selectfont}
%\renewcommand\Large{\fontsize{33.6}{43.2}\selectfont}
%\renewcommand\LARGE{\fontsize{40.8}{52.8}\selectfont}
%\renewcommand\huge{\fontsize{48}{60}\selectfont}
%\renewcommand\Huge{\fontsize{60}{72}\selectfont}

%%% Define some new fields for use in the header/title.

\def\class#1{\gdef\@class{#1}}
\def\@class{\@latex@warning@no@line{No \noexpand\class given}}

\def\year#1{\gdef\@year{#1}}
\def\@year{\@latex@warning@no@line{No \noexpand\year given}}

\def\department#1{\gdef\@department{#1}}
\def\@department{\@latex@warning@no@line{No \noexpand\department given}}

\def\advisor#1{\gdef\@advisor{#1}}
\def\@advisor{\@latex@warning@no@line{No \noexpand\advisor given}}
%\def\thanks#1{\gdef\@thanks{#1}}
%\def\@thanks{\@latex@warning@no@line{No \noexpand\thanks given}}

%%% Set up the header.

\newlength{\ignored@length}
\newcommand{\set@ess}[1]{%
  \renewcommand{\and}{%
    \global\def\@ess{s}%
  }%
  \global\def\@ess{}%
  \settowidth{\ignored@length}{#1}
}
\newcommand{\@andcommaspace}{, }

\RequirePackage{fancyhdr}
\pagestyle{fancy}
\fancyhf{}                      % Clear all fields.
%\fancyhead[L]{\@logo}
%\fancyhead[R]{\includegraphics{anotherfile}}

\fancyhead[C]{%
%{\begin{flushleft}{\@logo\\}\end{flushleft}}
{\color{black}\Large\textsf{\@class}\\}%
{\color{black}\fontsize{80pt}{100pt}\selectfont\textsf{\textbf{\@title}}\\[0.25em]}
{\color{black}\LARGE\textsf{\@author}\\[0.25em]}
}


%?? College Department of \@department.
%\@author}}
\fancyfoot[R]{\textbf{Special Thanks to:} Evimaria Terzi and Sofia Maria
Nikolakaki}
\fancyfoot[L]{\textbf{Instructor:}{Andrei Lapets}
}
\renewcommand{\headrulewidth}{2pt}
\renewcommand{\footrulewidth}{2pt}

%%% Make header and footer rules ltgray.
%%%
%%% This code taken from fancyhdr.sty; written by Piet van Oostrum.
\renewcommand{\footrule}{{\color{ltgray}\if@fancyplain\let\footrulewidth\plainfootrulewidth\fi
    \vskip-\footruleskip\vskip-\footrulewidth
    \hrule\@width\headwidth\@height\footrulewidth\vskip\footruleskip}}
\renewcommand{\headrule}{{\color{ltgray}\if@fancyplain\let\headrulewidth\plainheadrulewidth\fi
    \hrule\@height\headrulewidth\@width\headwidth \vskip-\headrulewidth}}
%%% End code from fancyhdr.sty.

%%% Make vertical rules ltgray.
%%%
%%% This code is modified from the code in multicol.sty; written by
%%% Frank Mittelbach and part of standard LaTeX.
%%%
%%% It will probably break if the original routine is modified in
%%% future versions of multicol.sty, so be sure to check there.

\renewcommand{\page@sofar}{%
   \process@cols\mult@rightbox
       {\ifvoid\count@
          \setbox\count@\hbox to\hsize{}%
        \else
          \wd\count@\hsize
        \fi}%
   \mult@info\z@
    {Column spec:\MessageBreak
       (\the\multicol@leftmargin\space -->
        \the\full@width\space = \the\hsize
        \space x \the\col@number)%
     }%
\ifvmode\else\errmessage{Multicol Error}\fi
   \moveright\multicol@leftmargin
    \hbox to\full@width{%
     \process@cols\mult@gfirstbox{\box\count@
       \hss{\color{ltgray}\vrule\@width\columnseprule}\hss}%
     \box\mult@rightbox
      \rlap{\phantom p}%
    }%
}
%%% End code from multicol.sty.

\setlength{\multicolsep}{2em}

\newenvironment{poster}{%
\thispagestyle{fancy}
%{\color{ltgray}\rule{\textwidth}{2pt}}
\begin{multicols*}{4}
}%
{\end{multicols*}
}

\renewenvironment{figure}
               {\def\@captype {figure}}
               {}

\renewenvironment{table}
               {\def\@captype {table}}
               {}


%%% Make section headers sans-serif.

\allsectionsfont{\sffamily}

%%% Suppress section numbering.
\renewcommand{\@seccntformat}[1]{\relax}

\endinput
