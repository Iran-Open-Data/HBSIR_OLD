from pathlib import Path

from matplotlib.font_manager import fontManager

def add_fonts():
    fonts_path = Path(__file__).parents[2].joinpath("fonts")
    try:
        fontManager.addfont(fonts_path.joinpath("xkcd", "xkcd-Regular.otf"))
        fontManager.addfont(fonts_path.joinpath("xkcd_scripts", "xkcd-script.ttf"))
        fontManager.addfont(fonts_path.joinpath("humor_sans", "Humor Sans.ttf"))
        fontManager.addfont(fonts_path.joinpath("comic_neue", "ComicNeue-Regular.ttf"))
    except ImportError:
        pass
